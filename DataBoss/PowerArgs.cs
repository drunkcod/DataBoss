using System;
using System.Collections;
using System.Collections.Generic;

namespace DataBoss.Specs
{
	public class PowerArgs : IEnumerable<KeyValuePair<string, string>>
	{
		readonly Dictionary<string, string> args = new Dictionary<string, string>();
		readonly List<string> commands = new List<string>(); 

		public int Count { get { return args.Count; } }

		public string this[string arg] { get { return args[arg]; } }

		public IReadOnlyList<string> Commands { get { return commands.AsReadOnly(); } }

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
				string value;
				if (TryGetArg(field.Name, out value))
					field.SetValue(target, value);
			}
		}

		private void FillProps(object target) {
			foreach (var prop in target.GetType().GetProperties()) {
				string value;
				if (TryGetArg(prop.Name, out value))
					prop.SetValue(target, value);
			}
		}

		public IEnumerator<KeyValuePair<string, string>> GetEnumerator() {
			return args.GetEnumerator();
		}

		IEnumerator IEnumerable.GetEnumerator() {
			return args.GetEnumerator();
		}
	}
}