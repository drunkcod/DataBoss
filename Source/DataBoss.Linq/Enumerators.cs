namespace DataBoss.Linq
{
	using System;
	using System.Collections.Generic;

	static public partial class Enumerators
	{
		public static List<T> ToList<T>(this IEnumerator<T> self) {
			var r = new List<T>();
			while (self.MoveNext())
				r.Add(self.Current);
			return r;
		}

		public static T[] ToArray<T>(this IEnumerator<T> self) {
			var items = new T[32];
			var n = 0;
			while(self.MoveNext()) {
				if (n == items.Length)
					Array.Resize(ref items, NextBufferSize(items.Length));
				items[n++] = self.Current;
			}
			Array.Resize(ref items, n);
			return items;
		}

		static int NextBufferSize(int size) => checked(size + size);

		public static IEnumerator<IReadOnlyList<T>> Batch<T>(this IEnumerator<T> items, int batchSize) {
			for (var it = Batch(items, () => new T[batchSize]); it.MoveNext();)
				yield return it.Current;		
		}

		public static IEnumerator<ArraySegment<T>> Batch<T>(this IEnumerator<T> items, Func<T[]> newBucket) {
			if (!items.MoveNext())
				yield break;
			var n = 0;
			var bucket = newBucket();
			do {
				bucket[n++] = items.Current;
				if (n == bucket.Length) {
					yield return new ArraySegment<T>(bucket);
					bucket = newBucket();
					n = 0;
				}
			} while (items.MoveNext());

			if (n != 0)
				yield return new ArraySegment<T>(bucket, 0, n);
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
}

#if NETSTANDARD2_1_OR_GREATER
namespace DataBoss.Linq
{
	using System;
	using System.Collections.Generic;
	using System.Buffers;

	static public partial class Enumerators
	{
		class MemorySegment<T> : IMemoryOwner<T>
		{
			readonly Memory<T> slice;
			readonly IDisposable source;

			public MemorySegment(Memory<T> slice, IDisposable source) {
				this.slice = slice;
				this.source = source;
			}

			public Memory<T> Memory => slice;

			public void Dispose() => source.Dispose();
		}

		public static IEnumerator<IMemoryOwner<T>> Batch<T>(this IEnumerator<T> items, MemoryPool<T> memory) => items.Batch(memory, -1);
		public static IEnumerator<IMemoryOwner<T>> Batch<T>(this IEnumerator<T> items, MemoryPool<T> memory, int minBufferSize = -1) {
			if (!items.MoveNext())
				yield break;
			var n = 0;
			var bucket = memory.Rent(minBufferSize);
			var buffer = bucket.Memory.Span;
			do {
				buffer[n++] = items.Current;
				if (n == buffer.Length) {
					yield return bucket;
					bucket = memory.Rent(minBufferSize);
					buffer = bucket.Memory.Span;
					n = 0;
				}
			} while (items.MoveNext());

			if (n != 0)
				yield return new MemorySegment<T>(bucket.Memory.Slice(0, n), bucket);
		}
	}
}
#endif