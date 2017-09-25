using System;
using System.Collections;
using System.Collections.Generic;

namespace DataBoss
{
	public class ConverterCollection : IEnumerable<Delegate>
	{
		KeyValuePair<Type, Delegate>[] converters;
		int count = 0;

		public ConverterCollection() {
			this.converters = new KeyValuePair<Type, Delegate>[8];
		}

		public ConverterCollection(ConverterCollection other) {
			if(other == null) {
				this.converters = new KeyValuePair<Type, Delegate>[0];
			} else {
				this.converters = new KeyValuePair<Type, Delegate>[other.count];
				this.count = other.count;
				Array.Copy(other.converters, this.converters, this.converters.Length);
			}
		}

		public void Add<TFrom, TTo>(Func<TFrom, TTo> converter) => Add(typeof(TFrom), converter);

		void Add(Type from, Delegate converter) {
			if(count == converters.Length)
				Array.Resize(ref converters, Math.Max(count << 1, 8));
			converters[count++] = new KeyValuePair<Type, Delegate>(from, converter);
		}

		public bool TryGetConverter(Type from, Type to, out Delegate converter) {
			var found = Array.FindIndex(converters, 0, count, x => x.Key == from && x.Value.Method.ReturnType == to);
			if(found != -1) {
				converter = converters[found].Value;
				return true;
			}
			converter = null;
			return false;
		}

		IEnumerator<Delegate> IEnumerable<Delegate>.GetEnumerator() => GetEnumeratorCore();
		IEnumerator IEnumerable.GetEnumerator() => GetEnumeratorCore();

		IEnumerator<Delegate> GetEnumeratorCore() {
			for(var i = 0; i != count; ++i)
				yield return converters[i].Value;
		}
	}
}