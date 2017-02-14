using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;

namespace DataBoss.Core
{
	static class MissinqLinq
	{
		public static TOutput[] ConvertAll<T,TOutput>(this T[] self, Converter<T,TOutput> converter) =>
			Array.ConvertAll(self, converter);

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

		public static IEnumerable<T> Select<T>(this Func<TextReader> self, Func<string,T> selector) {
			using(var text = self())
				for(string line; (line = text.ReadLine()) != null;)
					yield return selector(line);
		} 
	}

	public static class CustomAttributeProviderExtensions
	{
		public static bool Any<T>(this ICustomAttributeProvider attributes) where T : Attribute =>
			attributes.GetCustomAttributes(typeof(T), true).Length != 0;

		public static T Single<T>(this ICustomAttributeProvider attributes) where T : Attribute =>
			attributes.GetCustomAttributes(typeof(T), true).Cast<T>().Single();

		public static T SingleOrDefault<T>(this ICustomAttributeProvider attributes) where T : Attribute =>
			attributes.GetCustomAttributes(typeof(T), true).Cast<T>().SingleOrDefault();
	}
}