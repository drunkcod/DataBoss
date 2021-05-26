using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.IO;
using DataBoss.Buffers;

namespace DataBoss.IO
{
	public class ProducerStream : Stream
	{
		readonly ArrayPool<byte> chunkPool;
		readonly BlockingCollection<(byte[], int)> chunks;
		readonly int chunkSize;

		byte[] writeChunk;
		int writeOffset;
		int writeBytesLeft;

		class ConsumerStream : Stream {
			readonly ArrayPool<byte> chunkPool;
			readonly BlockingCollection<(byte[] Bytes, int Length)> chunks;

			byte[] chunk;
			int chunkOffset;
			int chunkLength;

			public ConsumerStream(ArrayPool<byte> chunkPool, BlockingCollection<(byte[], int)> chunks) {
				this.chunkPool = chunkPool;
				this.chunks = chunks;
				this.chunk = chunkPool.Rent(0);
			}

			public override bool CanRead => true;
			public override bool CanSeek => false;
			public override bool CanWrite => false;

			public override long Length => throw new NotSupportedException();

			public override long Position {
				get => throw new NotSupportedException();
				set => throw new NotSupportedException();
			}

			public override void Flush() => throw new NotSupportedException();

			public override int Read(byte[] buffer, int offset, int count) {
				if (count <= 0)
					return 0;

				var read = 0;
				do {
					if (!TryGetChunkBytes(-1))
						return read;

					var sliceSize = Math.Min(count - read, chunkLength - chunkOffset);
					Buffer.BlockCopy(
						chunk, chunkOffset,
						buffer, offset + read,
						sliceSize);

					read += sliceSize;
					chunkOffset += sliceSize;

				} while((read != count && TryGetChunkBytes(0)) || read == 0);

				return read;
			}

			bool TryGetChunkBytes(int millisecondsTimeout) {
				while (chunkLength == chunkOffset) {
					if (!chunks.TryTake(out var nextChunk, millisecondsTimeout))
						return false;
					chunkPool.Return(chunk);
					chunk = nextChunk.Bytes;
					chunkLength = nextChunk.Length;
					chunkOffset = 0;
				}
				return true;
			}

			public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
			public override void SetLength(long value) => throw new NotSupportedException();
			public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
		}

		public ProducerStream(int chunkSize = 8092) : this(ArrayPool<byte>.Create(), chunkSize) { }

		public ProducerStream(ArrayPool<byte> chunkPool, int chunkSize) {
			this.chunkPool = chunkPool;
			this.chunks = new();
			this.chunkSize = chunkSize;
			NewWriteChunk();
		}

		public Stream OpenConsumer() => new ConsumerStream(chunkPool, chunks);

		void NewWriteChunk() {
			this.writeChunk = chunkPool.Rent(chunkSize);
			this.writeOffset = 0;
			this.writeBytesLeft = writeChunk.Length;
		}

		public override bool CanRead => false;
		public override bool CanWrite => true;
		public override bool CanSeek => false;
		public override long Length => throw new NotSupportedException();
		public override long Position {
			get => throw new NotSupportedException();
			set => throw new NotSupportedException();
		}

		protected override void Dispose(bool disposing) => Close();

		public override void Close() {
			if (chunks.IsAddingCompleted)
				return;

			PublishChunk();
			chunks.CompleteAdding();
		}

		public override void Flush() {
			if(PublishChunk())
				NewWriteChunk();
		}

		bool PublishChunk() {
			if (writeOffset == 0)
				return false;

			chunks.Add((writeChunk, writeOffset));
			return true;
		}

		public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException("Can't write to ProducerStream, use: OpenConsumer()");

		public override void Write(byte[] buffer, int offset, int count) {
			while (count > 0) {
				if (writeBytesLeft == 0)
					Flush();

				var sliceSize = Math.Min(count, writeBytesLeft);

				Buffer.BlockCopy(
					buffer, offset,
					writeChunk, writeOffset,
					sliceSize);

				writeOffset += sliceSize;
				writeBytesLeft -= sliceSize;
				offset += sliceSize;
				count -= sliceSize;
			}
		}

		public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
		public override void SetLength(long value) => throw new NotSupportedException();
	}
}
