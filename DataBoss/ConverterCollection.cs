using System;
using System.Collections.Generic;

namespace DataBoss
{
	public class ConverterCollection
	{
		KeyValuePair<Type, Delegate>[] converters = new KeyValuePair<Type, Delegate>[8];
		int count = 0;

		public void Add<TFrom, TTo>(Func<TFrom, TTo> converter) => Add(typeof(TFrom), converter);

		void Add(Type from, Delegate converter) {
			if(count == converters.Length)
				Array.Resize(ref converters, count << 1);
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
	}
}