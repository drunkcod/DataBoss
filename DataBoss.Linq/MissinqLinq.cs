using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace DataBoss.Linq
{
	public static class MissingLinq
	{
		class CollectionAdapter<T, TItem> : IReadOnlyCollection<TItem>, ICollection<TItem>
		{
			readonly IReadOnlyCollection<T> items;
			readonly Func<T,TItem> selector;

			public CollectionAdapter(IReadOnlyCollection<T> items, Func<T, TItem> selector) {
				this.items = items;
				this.selector = selector;
			}

			public int Count => items.Count;
			public bool IsReadOnly => true;

			public void Add(TItem item) => throw new NotSupportedException();
			public void Clear() => throw new NotSupportedException();
			public bool Remove(TItem item) => throw new NotSupportedException();

			public bool Contains(TItem item) => items.Any(x => selector(x).Equals(item));

			public void CopyTo(TItem[] array, int arrayIndex) {
				if(array.Length < arrayIndex + items.Count)
					ThrowInsufficientSpaceException();
				foreach (var item in items)
					array[arrayIndex++] = selector(item);
			}

			public IEnumerator<TItem> GetEnumerator() => items.Select(selector).GetEnumerator();

			IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
			
			static void ThrowInsufficientSpaceException() => new int[1].CopyTo(new int[0], 0);
		}

		public static IReadOnlyCollection<T> AsReadOnly<T>(this IReadOnlyCollection<T> self) => AsReadOnly(self, Lambdas.Id<T>);
		public static IReadOnlyCollection<TItem> AsReadOnly<T, TItem>(this IReadOnlyCollection<T> self, Func<T, TItem> selector) =>
			new CollectionAdapter<T, TItem>(self, selector);

		public static IEnumerable<IReadOnlyList<T>> Batch<T>(this IEnumerable<T> items, int batchSize) =>
			Batch(items, () => new T[batchSize]);

		public static IEnumerable<IReadOnlyList<T>> Batch<T>(this IEnumerable<T> items, Func<T[]> newBucket) {
			T[] bucket = null;
			var n = 0;
			using (var it = items.GetEnumerator()) {
				if (!it.MoveNext())
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
			ChunkBy(items, selector, Lambdas.Id<TElement>);

		public static IEnumerable<IGrouping<TKey, TElement>> ChunkBy<T, TKey, TElement>(this IEnumerable<T> items, Func<T, TKey> keySelector, Func<T, TElement> elementSelector) {
			using (var x = items.GetEnumerator()) {
				if (!x.MoveNext())
					yield break;
				var c = keySelector(x.Current);
				var acc = new List<TElement>();
				for (; ; ) {
					acc.Add(elementSelector(x.Current));
					if (!x.MoveNext())
						break;
					var key = keySelector(x.Current);
					if (!c.Equals(key)) {
						yield return MakeArrayGrouping(c, acc.ToArray());
						c = key;
						acc.Clear();
					}
				}
				yield return MakeArrayGrouping(c, acc.ToArray());
			}
		}

		static ArrayGrouping<TKey, TElement> MakeArrayGrouping<TKey, TElement>(TKey key, TElement[] items) =>
			new ArrayGrouping<TKey, TElement>(items, key);

		public static void Consume<T>(this IEnumerable<T> items) {
			using (var xs = items.GetEnumerator())
				while (xs.MoveNext())
					;
		}

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

		public static IEnumerable<T> Inspect<T>(this IEnumerable<T> items, Action<T> inspectElement) {
			foreach(var item in items) {
				inspectElement(item);
				yield return item;
			}
		}

		public static bool IsSorted<T>(this IEnumerable<T> self) where T : IComparable<T> => IsSortedBy(self, id => id);

		public static bool IsSortedBy<T,TKey>(this IEnumerable<T> self, Func<T,TKey> selector) where TKey : IComparable<TKey> {
			using (var e = self.GetEnumerator()) {
				if (!e.MoveNext())
					return true;
				for (var prev = selector(e.Current); e.MoveNext();) {
					var c = selector(e.Current);
					if (prev.CompareTo(c) > 0)
						return false;
					prev = c;
				}
				return true;
			}
		}

		public static TValue SingleOrDefault<T, TValue>(this IEnumerable<T> items, Func<T, bool> predicate, Func<T, TValue> selector) {
			using (var it = items.GetEnumerator())
				while (it.MoveNext())
					if (predicate(it.Current)) {
						var found = it.Current;
						while (it.MoveNext())
							if (predicate(it.Current))
								ThrowTooManyElementsException();
						return selector(found);
					}
			return default(TValue);
		}

		static void ThrowTooManyElementsException() => Enumerable.Range(0, 2).SingleOrDefault(x => true);
	}
}