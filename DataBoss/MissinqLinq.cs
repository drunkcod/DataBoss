using System;
using System.Collections.Generic;

namespace DataBoss
{
	static class MissinqLinq
	{
		public static TOutput[] ConvertAll<T,TOutput>(this T[] self, Converter<T,TOutput> converter) {
			return Array.ConvertAll(self, converter);
		}

		public static void ForEach<T>(this IEnumerable<T> self, Action<T> action) {
			foreach(var item in self)
				action(item);
		}
	}
}