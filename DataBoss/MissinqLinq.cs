using System;
using System.Collections.Generic;
using System.IO;

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

	static class TextReaderExtensions
	{
		public static IEnumerable<string> AsEnumerable(this Func<TextReader> self) {
			using(var text = self())
				for(string line; (line = text.ReadLine()) != null;)
					yield return line;
		}

		public static IEnumerable<T> Select<T>(this Func<TextReader> self, Converter<string,T> selector) {
			using(var text = self())
				for(string line; (line = text.ReadLine()) != null;)
					yield return selector(line);
		} 
	}
}