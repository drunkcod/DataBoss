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
		public static void ForEach<T>(this IEnumerable<T> self, Action<T> action) {
			foreach(var item in self)
				action(item);
		}

		public static IEnumerable<IEnumerable<T>> Batch<T>(this IEnumerable<T> items, int batchSize) {
			var bucket = new T[batchSize];
			var n = 0;
			using(var it = items.GetEnumerator())
				while(it.MoveNext()) {
					bucket[n++] = it.Current;
					if(n == bucket.Length) { 
						yield return bucket;
						bucket = new T[batchSize];
						n = 0;
					}
				}
			if(n != 0)
				yield return new ArraySegment<T>(bucket, 0, n);
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

		public static TValue SingleOrDefault<T, TValue>(this IEnumerable<T> items, Func<T, bool> predicate, Func<T, TValue> selector) {
			using (var it = items.GetEnumerator())
				while (it.MoveNext())
					if (predicate(it.Current)) {
						var found = it.Current;
						while (it.MoveNext())
							if (predicate(it.Current))
								Enumerable.Range(0, 2).SingleOrDefault(x => true);
						return selector(found);
					}
			return default(TValue);
		}
	}

	public static class TextReaderExtensions
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

	public static class ArrayExtensions
	{
		public static TOutput[] ConvertAll<T, TOutput>(this T[] self, Converter<T, TOutput> converter) =>
			Array.ConvertAll(self, converter);

		public static T Single<T>(T[] ts) => ts.Length == 1 ? ts[0] : throw new InvalidOperationException("Array contains more than one element.");
	}
}