using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;

namespace DataBoss.Linq
{
	public static class MissingLinq
	{
		public static TOutput[] ConvertAll<T,TOutput>(this T[] self, Converter<T,TOutput> converter) =>
			Array.ConvertAll(self, converter);

		public static void ForEach<T>(this IEnumerable<T> self, Action<T> action) {
			foreach(var item in self)
				action(item);
		}

		class ArrayGrouping<TKey, TElement> : IGrouping<TKey, TElement>
		{
			readonly TElement[] items;
			readonly TKey key;

			public ArrayGrouping(TElement[] items, TKey key) {
				this.items = items;
				this.key = key;
			}

			public TKey Key => key;
			public IEnumerator<TElement> GetEnumerator() => ((IEnumerable<TElement>)items).GetEnumerator();
			IEnumerator IEnumerable.GetEnumerator() => items.GetEnumerator();
		}

		public static IEnumerable<IGrouping<TKey, TElement>> ChunkBy<TElement, TKey>(this IEnumerable<TElement> items, Func<TElement, TKey> selector) { 
			using (var x = items.GetEnumerator())
			{
				if (!x.MoveNext())
					yield break;
				var c = selector(x.Current);
				var acc = new List<TElement>();
				for (;;)
				{
					acc.Add(x.Current);
					if (!x.MoveNext())
						break;
					var key = selector(x.Current);
					if (!c.Equals(key))
					{
						yield return new ArrayGrouping<TKey, TElement>(acc.ToArray(), c);
						c = key;
						acc.Clear();
					}
				}
				yield return new ArrayGrouping<TKey, TElement>(acc.ToArray(), c);
			}
		}

		public static void Consume<T>(this IEnumerable<T> items) {
			using(var xs = items.GetEnumerator())
				while(xs.MoveNext())
					;
		}

		public static IEnumerable<T> Inspect<T>(this IEnumerable<T> items, Action<T> inspectElement) {
			foreach(var item in items) {
				inspectElement(item);
				yield return item;
			}
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