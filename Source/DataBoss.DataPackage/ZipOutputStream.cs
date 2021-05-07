using System;
using System.IO;

namespace DataBoss.DataPackage
{
	class ZipOutputStream : Stream
	{
		readonly Stream stream;
		long position;

		public ZipOutputStream(Stream stream) {
			this.stream = stream;
		}

		protected override void Dispose(bool disposing) {
			stream.Dispose();
			base.Dispose(disposing);
		}

		public override bool CanRead => false;
		public override bool CanWrite => true;
		public override bool CanSeek => false;

		public override long Length => throw new NotSupportedException();

		public override long Position { get => position; set => throw new NotSupportedException(); }

		public override void Flush() => stream.Flush();

		public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();

		public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();

		public override void SetLength(long value) => throw new NotSupportedException();
		public override void Write(byte[] buffer, int offset, int count) {
			stream.Write(buffer, offset, count);
			position += count;
		}
	}
}
