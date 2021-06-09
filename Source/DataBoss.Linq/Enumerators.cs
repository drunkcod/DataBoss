using System;
using System.Collections.Generic;

namespace DataBoss.Linq
{
	public static class Enumerators
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