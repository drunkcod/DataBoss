using System;
using System.Collections.Generic;

namespace DataBoss
{
	public static class DictionaryExtensions
	{
		public static TValue GetOrAdd<TKey,TValue>(this IDictionary<TKey,TValue> self, TKey key, Func<TKey,TValue> valueFactory) {
			TValue found;
			if(!self.TryGetValue(key, out found)) {
				found = valueFactory(key);
				self.Add(key, found);
			}
			return found;
		}
	}
}