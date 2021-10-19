using System;
using System.Buffers;

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
}
