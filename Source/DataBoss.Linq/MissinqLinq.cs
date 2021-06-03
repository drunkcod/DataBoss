using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using DataBoss.Collections;

namespace DataBoss.Linq
{
	public class EmptyCollection<T> : ICollection<T>, IReadOnlyCollection<T>, IEnumerator<T>
	{
		T IEnumerator<T>.Current => throw new InvalidOperationException();
		object IEnumerator.Current => throw new InvalidOperationException();
		bool IEnumerator.MoveNext() => false;
		void IEnumerator.Reset() { }
		void IDisposable.Dispose() { }

		public int Count => 0;
		public bool IsReadOnly => true;

		public void Add(T item) => throw new InvalidOperationException();
		public void Clear() => throw new InvalidOperationException();
		public bool Remove(T item) => throw new InvalidOperationException();

		public bool Contains(T item) => false;
		public void CopyTo(T[] array, int arrayIndex) { }

		public IEnumerator<T> GetEnumerator() => this;
		IEnumerator IEnumerable.GetEnumerator() => this;

		internal EmptyCollection() { }
	}

	public static class Collection
	{
		public static EmptyCollection<T> Empty<T>() => new EmptyCollection<T>();
	}

	public static class Enumerators
	{
		public static List<T> ToList<T>(this IEnumerator<T> self) {
			var r = new List<T>();
			while (self.MoveNext())
				r.Add(self.Current);
			return r;
		}

		public static T[] ToArray<T>(this IEnumerator<T> self) {
			var items = new T[16];
			var n = 0;
			while(self.MoveNext()) {
				if (n == items.Length)
					Array.Resize(ref items, items.Length * 2);
				items[n++] = self.Current;
			}
			Array.Resize(ref items, n);
			return items;			
		}

		public static int Count<T>(this IEnumerator<T> self) {
			var n = 0;
			while (self.MoveNext())
				++n;
			return n;
		}

		public static T First<T>(this IEnumerator<T> self) {
			if (!self.MoveNext())
				return MissingLinq.ThrowNoElements<T>();
			return self.Current;
		}

		public static T Single<T>(this IEnumerator<T> self) {
			if (!self.MoveNext())
				return MissingLinq.ThrowNoElements<T>();
			var found = self.Current;
			if (self.MoveNext())
				return MissingLinq.ThrowMoreThanOneElement<T>();
			return found;
		}
	}

	public static class MissingLinq
	{
		static internal T ThrowNoElements<T>() => new T[0].Single();
		static internal T ThrowMoreThanOneElement<T>() => Enumerable.Repeat(default(T), 2).Single();
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

		static KeyValuePair<TKey, TValue> KeyValuePair<TKey, TValue>(TKey key, TValue value) => new KeyValuePair<TKey, TValue>(key, value);

		public static IEnumerable<KeyValuePair<TKey, TAcc>> AggregateBy<T, TKey, TAcc>(this IEnumerable<T> items, Func<T, TKey> keySelector, Func<T, TAcc> getAccumulator, Func<TAcc, T, TAcc> accumulate) {
			var r = new List<KeyValuePair<TKey, TAcc>>();
			var pos = new Dictionary<TKey, int>();
			foreach (var item in items) {
				var key = keySelector(item);
				if (pos.TryGetValue(key, out var n))
					r[n] = KeyValuePair(key, accumulate(r[n].Value, item));
				else {
					n = r.Count;
					pos.Add(key, n);
					r.Add(KeyValuePair(key, getAccumulator(item)));
				}
			}
			return r;
		}

		public static IReadOnlyCollection<T> AsReadOnly<T>(this IReadOnlyCollection<T> self) => self;
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
			ChunkBy(items, selector, Lambdas.Id);

		public static IEnumerable<IGrouping<TKey, TElement>> ChunkBy<T, TKey, TElement>(this IEnumerable<T> items, Func<T, TKey> keySelector, Func<T, TElement> elementSelector) {
			using var x = items.GetEnumerator(); 
			
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

		static ArrayGrouping<TKey, TElement> MakeArrayGrouping<TKey, TElement>(TKey key, TElement[] items) =>
			new ArrayGrouping<TKey, TElement>(items, key);

		public static void Consume<T>(this IEnumerable<T> items) {
			using var xs = items.GetEnumerator();
			while (xs.MoveNext())
				;
		}

		public static TOutput[] ToArray<T, TOutput>(this IReadOnlyCollection<T> self, Func<T, TOutput> selector) {
			var r = new TOutput[self.Count];
			var n = 0;
			foreach(var item in self)
				r[n++] = selector(item);
			return r;
		}

		public static IEnumerable<T> EmptyIfNull<T>(this IEnumerable<T> items) => 
			items ?? Enumerable.Empty<T>();
		
		public static void ForEach<T>(this IEnumerable<T> self, Action<T> action) {
			foreach(var item in self)
				action(item);
		}

		public static IEnumerable<T> Filter<T>(this IEnumerable<T> self, IEnumerable<bool> bs) {
			using var items = self.GetEnumerator();
			using var include = bs.GetEnumerator();
			while (items.MoveNext() && include.MoveNext())
				if (include.Current)
					yield return items.Current;
		}

		public static IEnumerable<T> Inspect<T>(this IEnumerable<T> items, Action<T> inspectElement) {
			foreach(var item in items) {
				inspectElement(item);
				yield return item;
			}
		}

		public static bool IsEmpty<T>(this IEnumerable<T> self) => !self.Any();
		public static bool IsEmpty<T>(this IReadOnlyCollection<T> self) => self.Count == 0;

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

		public static BlockCollection<T> ToCollection<T>(this IEnumerable<T> items) {
			var c = new BlockCollection<T>();
			foreach(var item in items)
				c.Add(item);
			return c;
		}

		public static HashSet<T> ToHashSet<T>(this IEnumerable<T> items) => new HashSet<T>(items);

		static void ThrowTooManyElementsException() => Enumerable.Range(0, 2).SingleOrDefault(x => true);
	}
}