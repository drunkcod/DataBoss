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
				var item = it.Current;
				if(IsArg(item)) {
					item = item.Substring(1);
					if(!it.MoveNext() || it.Current.StartsWith("-"))
						throw new InvalidOperationException("No value given for '" + item + "'");
					var value = it.Current;

					result.Add(item, value);
				} else {
					result.commands.Add(item);
				}
			}
			return result;
		}

		static bool IsArg(string item) {
			return item.StartsWith("-");
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