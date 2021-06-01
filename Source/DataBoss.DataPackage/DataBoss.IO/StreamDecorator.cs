using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace DataBoss.IO
{
	class StreamDecorator : Stream
	{
		readonly Stream inner;

		public StreamDecorator(Stream stream) { this.inner = stream;  }

		public event Action Closed;

		public override bool CanRead => inner.CanRead;
		public override bool CanSeek => inner.CanSeek;
		public override bool CanWrite => inner.CanWrite;
		public override long Length => inner.Length;

		public override long Position { 
			get => inner.Position; 
			set => inner.Position = value; 
		}

		public override void Close() {
			try {
				inner.Close();
			} finally {
				Closed?.Invoke();
			}
		}

		protected override void Dispose(bool disposing) {
			if (disposing)
				inner.Dispose();
		}

		public override void Flush() => inner.Flush();

		public override int Read(byte[] buffer, int offset, int count) => inner.Read(buffer, offset, count);
		public override int ReadByte() => inner.ReadByte();

		public override long Seek(long offset, SeekOrigin origin) => inner.Seek(offset, origin);
		public override void SetLength(long value) => inner.SetLength(value);

		public override void Write(byte[] buffer, int offset, int count) => inner.Write(buffer, offset, count);
		public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) => inner.WriteAsync(buffer, offset, count, cancellationToken);
		public override void WriteByte(byte value) => inner.WriteByte(value);

#if NETSTANDARD2_1_OR_GREATER
		public override int Read(Span<byte> buffer) => inner.Read(buffer);
		public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) => inner.ReadAsync(buffer, offset, count, cancellationToken);
		public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default) => inner.ReadAsync(buffer, cancellationToken);

		public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default) => inner.WriteAsync(buffer, cancellationToken);
		public override void Write(ReadOnlySpan<byte> buffer) => inner.Write(buffer);
#endif
	}
}
