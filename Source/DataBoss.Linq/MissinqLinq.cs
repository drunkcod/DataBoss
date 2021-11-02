namespace DataBoss.Linq
{
	using System;
	using System.Collections;
	using System.Collections.Generic;
	using System.Linq;
	using DataBoss.Collections;

	static public partial class MissingLinq
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
		}

		class CollectionReadOnlyAdapter<T> : IReadOnlyCollection<T>, ICollection<T>
		{
			readonly ICollection<T> items;

			public CollectionReadOnlyAdapter(ICollection<T> items) {
				this.items = items;
			}

			public int Count => items.Count;
			public bool IsReadOnly => true;

			public void Add(T item) => throw new NotSupportedException();
			public void Clear() => throw new NotSupportedException();
			public bool Remove(T item) => throw new NotSupportedException();

			public bool Contains(T item) => items.Any(x => x.Equals(item));

			public void CopyTo(T[] array, int arrayIndex) {
				if (array.Length < arrayIndex + items.Count)
					ThrowInsufficientSpaceException();
				foreach (var item in items)
					array[arrayIndex++] = item;
			}

			public IEnumerator<T> GetEnumerator() => items.GetEnumerator();
			IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
		}

		static void ThrowInsufficientSpaceException() => new int[1].CopyTo(new int[0], 0);

		static KeyValuePair<TKey, TValue> KeyValuePair<TKey, TValue>(TKey key, TValue value) => new(key, value);

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

		public static IReadOnlyCollection<T> ToReadOnly<T>(this IEnumerable<T> self) => 
			self switch {
				IReadOnlyCollection<T> x => x,
				ICollection<T> x => new CollectionReadOnlyAdapter<T>(x),
				_ => self.ToList().AsReadOnly(),
			};

		public static IEnumerable<IReadOnlyList<T>> Batch<T>(this IEnumerable<T> items, int batchSize) =>
			Batch(items, () => new T[batchSize]).Cast<IReadOnlyList<T>>();

		public static IEnumerable<ArraySegment<T>> Batch<T>(this IEnumerable<T> self, Func<T[]> newBucket) {			
			using var it = self.GetEnumerator();
			for(var items = it.Batch(newBucket); items.MoveNext();)
				yield return items.Current;
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

		static ArrayGrouping<TKey, TElement> MakeArrayGrouping<TKey, TElement>(TKey key, TElement[] items) => new(items, key);

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
			using var e = self.GetEnumerator(); 
			
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

		public static TValue SingleOrDefault<T, TValue>(this IEnumerable<T> items, Func<T, bool> predicate, Func<T, TValue> selector) {
			using (var it = items.GetEnumerator())
				while (it.MoveNext())
					if (predicate(it.Current)) {
						var found = it.Current;
						while (it.MoveNext())
							if (predicate(it.Current))
								ThrowMoreThanOneMatch();
						return selector(found);
					}
			return default;
		}

		static internal T ThrowNoElements<T>() => new T[0].Single();
		static internal T ThrowMoreThanOneElement<T>() => new T[2].Single();
		static void ThrowMoreThanOneMatch() => new int[2].SingleOrDefault(x => true);

		public static BlockCollection<T> ToCollection<T>(this IEnumerable<T> items) {
			var c = new BlockCollection<T>();
			foreach(var item in items)
				c.Add(item);
			return c;
		}
	}
}

#if NETSTANDARD2_0
namespace DataBoss.Linq
{
	using System.Collections.Generic;
	
	public partial class MissingLinq
	{
		public static HashSet<T> ToHashSet<T>(this IEnumerable<T> items) => new(items);
	}
}
#endif

#if NETSTANDARD2_1_OR_GREATER
namespace DataBoss.Linq
{
	using System.Buffers;
	using System.Collections.Generic;

	static public partial class MissingLinq
	{
		public static IEnumerable<IMemoryOwner<T>> Batch<T>(this IEnumerable<T> self, MemoryPool<T> memory) => Batch(self, memory, -1);
		public static IEnumerable<IMemoryOwner<T>> Batch<T>(this IEnumerable<T> self, MemoryPool<T> memory, int minBufferSize = -1) {
			using var it = self.GetEnumerator();
			for (var items = it.Batch(memory, minBufferSize); items.MoveNext();)
				yield return items.Current;
		}
	}
}
#endif