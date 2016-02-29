using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Reflection;

namespace DataBoss
{
	public class PowerArgsValidationResult
	{
		public PowerArgsValidationResult(string name, Type type, object value, ValidationAttribute validation)
		{
			this.Name = name;
			this.ArgType = type;
			this.Value = value;
			this.Validation = validation;
		}

		public readonly string Name;
		public readonly Type ArgType;
		public string Message => Validation.FormatErrorMessage(Name);
		public readonly object Value;
		public ValidationAttribute Validation;
	}

	public class PowerArgsValidationException : Exception
	{
		readonly IReadOnlyCollection<PowerArgsValidationResult> errors;

		public PowerArgsValidationException(ICollection<PowerArgsValidationResult> errors) {
			this.errors = errors.ToList().AsReadOnly();
		}

		public IReadOnlyCollection<PowerArgsValidationResult> Errors => errors;
	}

	public class PowerArgs : IEnumerable<KeyValuePair<string, string>>
	{
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
			for(var it = args.GetEnumerator(); it.MoveNext();) {
				string item;
				if(MatchArg(it.Current, out item)) {
					if(!it.MoveNext() || IsArg(it.Current))
						throw new InvalidOperationException("No value given for '" + item + "'");
					result.Add(item, it.Current);
				} else {
					result.commands.Add(it.Current);
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

		static bool MatchArg(string item, out string result) {
			if(item.StartsWith("-")) {
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

		public T Into<T>() where T : new() {
			return Into(new T());
		}

		public T Into<T>(T target) {
			FillFields(target);
			FillProps(target);
			return target;
		}

		private void FillFields(object target) {
			foreach (var field in target.GetType().GetFields()) {
				object value;
				if (TryGetArgWithDefault(field.Name, field, field.FieldType, out value))
					field.SetValue(target, value);
			}
		}

		private void FillProps(object target) {
			foreach (var prop in target.GetType().GetProperties()) {
				object value;
				if (TryGetArgWithDefault(prop.Name, prop, prop.PropertyType, out value))
					prop.SetValue(target, value);
			}
		}

		private bool TryGetArgWithDefault(string name, MemberInfo member, Type targetType, out object value) {
			string argValue;
			if(TryGetArg(name, out argValue) && TryParse(argValue, targetType, out value))
				return true;

			var defalt = member.GetCustomAttribute<DefaultValueAttribute>();
			if(defalt != null) {
				value = defalt.Value;
				return true;
			}
			value = null;
			return false;
		}

		bool TryParse(string input, Type targetType, out object result) {
			if(targetType == typeof(string)) {
				result = input;
				return true;
			}
			if(targetType == typeof(DateTime)) {
				DateTime value;
				if(DateTime.TryParse(input, out value)) {
					result = value;
					return true;
				}
			}
			if(targetType.IsGenericType && targetType.GetGenericTypeDefinition() == typeof(Nullable<>)) {
				object inner;
				var t = targetType.GetGenericArguments()[0];
				if(TryParse(input, t, out inner)) {
					result = targetType.GetConstructor(new[] { t }).Invoke(new[] { inner });
					return true;
				}
			}
			result = null;
			return false;
		}

		public IEnumerator<KeyValuePair<string, string>> GetEnumerator() {
			return args.GetEnumerator();
		}

		IEnumerator IEnumerable.GetEnumerator() {
			return args.GetEnumerator();
		}
	}
}