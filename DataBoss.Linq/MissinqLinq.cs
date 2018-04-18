using System;
using System.Collections.Generic;
using System.Linq;

namespace DataBoss.Linq
{
	public static class MissingLinq
	{
		public static TOutput[] ConvertAll<T, TOutput>(this IReadOnlyCollection<T> self, Converter<T, TOutput> converter) {
			var r = new TOutput[self.Count];
			var n = 0;
			foreach(var item in self)
				r[n++] = converter(item);
			return r;
		}
		public static void ForEach<T>(this IEnumerable<T> self, Action<T> action) {
			foreach(var item in self)
				action(item);
		}

		public static IEnumerable<IEnumerable<T>> Batch<T>(this IEnumerable<T> items, int batchSize) =>
			Batch(items, () => new T[batchSize]);

		public static IEnumerable<IEnumerable<T>> Batch<T>(this IEnumerable<T> items, Func<T[]> newBucket) {
			T[] bucket = null;
			var n = 0;
			using (var it = items.GetEnumerator()) {
				if(!it.MoveNext())
					yield break;
				bucket = newBucket();
				do {
					bucket[n++] = it.Current;
					if (n == bucket.Length) {
						yield return bucket;
						bucket = newBucket();
						n = 0;
					}
				} while (it.MoveNext());
			}
			if (n != 0)
				yield return new ArraySegment<T>(bucket, 0, n);
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
}