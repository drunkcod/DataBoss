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

		class ArrayGrouping<TKey, TElement> : IGrouping<TKey, TElement>, 
			ICollection<TElement>//To enable System.Linq.Enumerable fast-paths.
		{
			readonly TElement[] items;
			readonly TKey key;

			public ArrayGrouping(TElement[] items, TKey key) {
				this.items = items;
				this.key = key;
			}

			public TKey Key => key;
			public int Count => items.Length;
			public bool IsReadOnly => true;

			public void Add(TElement item) => throw NotSupported();
			public void Clear() => throw NotSupported();
			public bool Remove(TElement item) => throw NotSupported();

			public bool Contains(TElement item) => Array.IndexOf(items, item) != -1;
			public void CopyTo(TElement[] array, int arrayIndex) => items.CopyTo(array, arrayIndex);

			public IEnumerator<TElement> GetEnumerator() => ((IEnumerable<TElement>)items).GetEnumerator();
			IEnumerator IEnumerable.GetEnumerator() => items.GetEnumerator();

			static InvalidOperationException NotSupported() => new InvalidOperationException("ArrayGrouping IsReadonly");
		}

		public static IEnumerable<IGrouping<TKey, TElement>> ChunkBy<TElement, TKey>(this IEnumerable<TElement> items, Func<TElement, TKey> selector) =>
			ChunkBy(items, selector, id => id);

		public static IEnumerable<IGrouping<TKey, TElement>> ChunkBy<T, TKey, TElement>(this IEnumerable<T> items, Func<T, TKey> keySelector, Func<T, TElement> elementSelector) { 
			using (var x = items.GetEnumerator()) {
				if (!x.MoveNext())
					yield break;
				var c = keySelector(x.Current);
				var acc = new List<TElement>();
				for (;;) {
					acc.Add(elementSelector(x.Current));
					if (!x.MoveNext())
						break;
					var key = keySelector(x.Current);
					if (!c.Equals(key)) {
						yield return MakeArrayGrouping(c, acc);
						c = key;
						acc.Clear();
					}
				}
				yield return MakeArrayGrouping(c, acc);
			}
		}

		static ArrayGrouping<TKey, TElement> MakeArrayGrouping<TKey, TElement>(TKey key, IEnumerable<TElement> items) =>
			new ArrayGrouping<TKey, TElement>(items.ToArray(), key);

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