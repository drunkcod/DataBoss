using System;
using System.Buffers;
using System.Collections.Generic;
using System.Linq;
using CheckThat;
using Xunit;

namespace DataBoss.Linq
{
	class TestMemoryPool<T> : MemoryPool<T> 
	{
		public override int MaxBufferSize => int.MaxValue;
		public int DefaultBufferSize = 16;

		class TestMemoryOwner : IMemoryOwner<T> {
			public Memory<T> Memory { get; internal set; }

			public void Dispose() { }
		}

		public override IMemoryOwner<T> Rent(int minBufferSize = -1) {
			return new TestMemoryOwner {
				Memory = new T[minBufferSize == -1 ? DefaultBufferSize : minBufferSize]
			};
		}

		protected override void Dispose(bool disposing) { }
	}

	public class Enumerators_
	{
		[Fact]
		public void ToArray() => Check.That(
			() => Items(1).GetEnumerator().ToArray().SequenceEqual(Items(1)),
			() => Items(17).GetEnumerator().ToArray().SequenceEqual(Items(17)));

		[Fact]
		public void Batch_MemoryPool() {
			var items = Items(97);
			var memory = new TestMemoryPool<int> { DefaultBufferSize = 10 };
			var batches = items.GetEnumerator().Batch(memory).ToList();
			Check.That(
				() => batches.Count == 10,
				() => batches.Sum(x => x.Memory.Length) == items.Count,
				() => batches.SelectMany(x => x.Memory.ToArray()).SequenceEqual(items));
		}

		static ICollection<int> Items(int count) => Enumerable.Range(0, count).Select(x => x).ToList();
	}
}
