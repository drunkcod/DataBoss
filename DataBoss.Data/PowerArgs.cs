using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text.RegularExpressions;

namespace DataBoss
{
	public class PowerArgsParseException : Exception
	{
		public class ParseFailure
		{
			public string ArgumentName;
			public Type ArgumentType;
			public string Input;
			public Exception Error;
		}

		public readonly IReadOnlyList<ParseFailure> Errors;

		internal PowerArgsParseException(IReadOnlyList<ParseFailure> errors) {
			this.Errors = errors;
		}
	}

	public class PowerArgs : IEnumerable<KeyValuePair<string, string>>
	{
		static readonly ConcurrentDictionary<Type, Action<object, string>> AddItemCache = new ConcurrentDictionary<Type, Action<object, string>>();

		readonly Dictionary<string, string> args = new Dictionary<string, string>();
		readonly List<string> commands = new List<string>(); 

		public int Count => args.Count;

		public string this[string arg] => args[arg];

		public IReadOnlyList<string> Commands => commands.AsReadOnly();

		public static PowerArgs Parse(params string[] args) {
			return Parse((IEnumerable<string>)args);
		}

		public static PowerArgs Parse(IEnumerable<string> args) {
			var result = new PowerArgs();
			using(var it = args.GetEnumerator()) {
				for(string item = null; it.MoveNext();) {
					if(item != null && result.args[item].EndsWith(",") || it.Current.StartsWith(",")) {
						result.args[item] = result.args[item] + it.Current;
					} else if(MatchArg(it.Current, out item)) {
						if(!it.MoveNext() || IsArg(it.Current))
							throw new InvalidOperationException("No value given for '" + item + "'");
						result.Add(item, it.Current);
					} else {
						result.commands.Add(it.Current);
					}
				}
			}
			return result;
		}

		public static void Validate(object obj) {
			var errors = new List<PowerArgsValidationResult>();
			foreach (var field in obj.GetType().GetFields()) {
				var validations = field.GetCustomAttributes<ValidationAttribute>();
				var value = field.GetValue(obj);
				foreach(var item in validations)
					if(!item.IsValid(value))
						errors.Add(new PowerArgsValidationResult(field.Name, field.FieldType, value, item));
			}
			foreach (var prop in obj.GetType().GetProperties()) {
				var validations = prop.GetCustomAttributes<ValidationAttribute>();
				var value = prop.GetValue(obj);
				foreach(var item in validations)
					if(!item.IsValid(value))
						errors.Add(new PowerArgsValidationResult(prop.Name, prop.PropertyType, value, item));
			}
			if(errors.Count > 0)
				throw new PowerArgsValidationException(errors);
		}

		public static List<PowerArg> Describe(Type argsType) { 
			var args = argsType.GetFields()
			.Cast<MemberInfo>()
			.Concat(argsType.GetProperties())
			.Select(x => new PowerArg(x))
			.ToList();

			args.Sort((a, b) => {
				if(a.Order.HasValue)
					return b.Order.HasValue ? a.Order.Value - b.Order.Value : -1;
				return b.Order.HasValue ? 1 : 0;
			});
			
			return args;
		}

		static bool MatchArg(string item, out string result) {
			if(Regex.IsMatch(item , "^-[^0-9]")) {
				result = item.Substring(1);
				return true;
			}
			result = null;
			return false;
		}

		static bool IsArg(string input) {
			string ignored;
			return MatchArg(input, out ignored);
		}

		void Add(string arg, string value) {
			args.Add(arg, value);
		}

		public bool TryGetArg(string name, out string value) {
			return args.TryGetValue(name, out value);
		}

		public T Into<T>() where T : new() => Into(new T());

		public T Into<T>(T target) {
			var parseFailures = new List<PowerArgsParseException.ParseFailure>();
			FillFields(target, parseFailures.Add);
			FillProps(target, parseFailures.Add);
			if(parseFailures.Count != 0)
				throw new PowerArgsParseException(parseFailures);
			return target;
		}

		private void FillFields(object target, Action<PowerArgsParseException.ParseFailure> onFailure) {
			foreach (var field in target.GetType().GetFields()) {
				object value;
				if (TryGetArgWithDefault(field, field.FieldType, onFailure, out value))
					field.SetValue(target, value);
			}
		}

		private void FillProps(object target, Action<PowerArgsParseException.ParseFailure> onFailure) {
			foreach (var prop in target.GetType().GetProperties()) {
				object value;
				if (TryGetArgWithDefault(prop, prop.PropertyType, onFailure, out value))
					prop.SetValue(target, value);
			}
		}

		private bool TryGetArgWithDefault(MemberInfo member, Type targetType, Action<PowerArgsParseException.ParseFailure> onFailure, out object value) {
			string argValue;
			if(TryGetArg(member.Name, out argValue) && TryParse(member.Name, argValue, targetType, onFailure, out value))
				return true;

			var defalt = member.GetCustomAttribute<DefaultValueAttribute>();
			if(defalt != null) {
				value = defalt.Value;
				return true;
			}
			value = null;
			return false;
		}

		bool TryParse(string name, string input, Type targetType, Action<PowerArgsParseException.ParseFailure> onFailure, out object result) {
			if(targetType == typeof(string)) {
				result = input;
				return true;
			}

			void ReportFailure(string s, Exception ex) =>
				onFailure(new PowerArgsParseException.ParseFailure {
					ArgumentName = name,
					ArgumentType = targetType,
					Input = s,
					Error = ex,
				});
			
			if (targetType.IsEnum) {
				try {
					result = Enum.Parse(targetType, input);
					return true;
				} catch(Exception ex) {
					result = null;
					ReportFailure(input, ex);
					return false;
				}
			}
			var hasParse = targetType.GetMethod("Parse", new []{ typeof(string) } );
			if(hasParse != null) {
				try {
					result = hasParse.Invoke(null, new object[] { input });
					return true;
				}
				catch(Exception ex) {
					result = null;
					ReportFailure(input, ex);
					return false;
				}
			}
			if(targetType.TryGetNullableTargetType(out var t)) {
				object inner;
				if(TryParse(name, input, t, onFailure, out inner)) {
					result = targetType.GetConstructor(new[] { t }).Invoke(new[] { inner });
					return true;
				}
			}

			Action<object, string> addItem;
			if(TryGetAddItem(targetType, out addItem)) {
				result = targetType.GetConstructor(Type.EmptyTypes).Invoke(new object[0]);
				foreach(var item in input.Split(','))
					try {
						addItem(result, item);
					} catch(Exception ex) {
						ReportFailure(item, ex);
					}
				return true;
			}
			result = null;
			return false;
		}

		static bool TryGetAddItem(Type targetType, out Action<object, string> output)
		{
			output = AddItemCache.GetOrAdd(targetType, targetTyp => {
				var adder = targetType
					.GetMethods(BindingFlags.Instance | BindingFlags.Public)
					.Where(x => x.Name == "Add")
					.Select(x => new { Add = x, Parameters = x.GetParameters() })
					.FirstOrDefault(x => x.Parameters.Length == 1);

				var parse = adder?.Parameters[0].ParameterType.GetMethod("Parse", new [] { typeof(string) });
				if(parse == null)
					return null;
				
				var target = Expression.Parameter(typeof(object));
				var value = Expression.Parameter(typeof(string));
				return Expression.Lambda<Action<object,string>>(
						Expression.Call(
							Expression.Convert(target, targetType), adder.Add, 
							Expression.Call(null,parse, value)), 
						target, value)
					.Compile();
			});

			return output != null;
		}

		public IEnumerator<KeyValuePair<string, string>> GetEnumerator() {
			return args.GetEnumerator();
		}

		IEnumerator IEnumerable.GetEnumerator() {
			return args.GetEnumerator();
		}
	}
}