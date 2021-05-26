using System;
using System.Buffers;
using System.Collections.Concurrent;

namespace DataBoss.Buffers
{
	public sealed class NullArrayPool<T> : ArrayPool<T>
	{
		public static readonly ArrayPool<T> Instance = new NullArrayPool<T>();

		public override T[] Rent(int minimumLength) {
			if (minimumLength == 0)
				return Array.Empty<T>();
			return new T[minimumLength];
		}

		public override void Return(T[] array, bool clearArray = false) { }
	}

	public sealed class SingleBucketArrayPool<T> : ArrayPool<T>
	{
		readonly ConcurrentStack<T[]> blocks = new();
		readonly int blockSize;

		public SingleBucketArrayPool(int blockSize) {
			this.blockSize = blockSize;
		}

		public override T[] Rent(int minimumLength) {
			if (minimumLength == 0)
				return Array.Empty<T>();
			return blocks.TryPop(out var found) ? found:  new T[blockSize];
		}

		public override void Return(T[] array, bool clearArray = false) {
			if (array.Length != blockSize)
				return;

			if (clearArray)
				for (var i = 0; i != array.Length; ++i)
					array[i] = default;

			blocks.Push(array);
		}
	}
}
