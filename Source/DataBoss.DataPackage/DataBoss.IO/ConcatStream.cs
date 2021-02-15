using System;
using System.Collections.Generic;
using System.IO;

namespace DataBoss.IO
{
	class ConcatStream : Stream
	{
		Stream activeStream;
		readonly IEnumerator<Stream> streams;

		public ConcatStream(IEnumerator<Stream> streams) {
			this.streams = streams;
			this.activeStream = Null;
		}

		public override bool CanRead => true;
		public override bool CanSeek => false;
		public override bool CanWrite => false;

		public override long Length => throw new NotSupportedException();
		public override long Position { 
			get => throw new NotSupportedException(); 
			set => throw new NotSupportedException();
		}

		public override int Read(byte[] buffer, int offset, int count) {
			var totalBytes = 0;
			do {
				var bytesRead = activeStream.Read(buffer, offset, count);
				totalBytes += bytesRead;
				if (bytesRead == count)
					break;

				offset += bytesRead;
				count -= bytesRead;
			} while (MoveNext());

			return totalBytes;
		}

		bool MoveNext() {
			activeStream.Dispose();
			if(streams.MoveNext()) {
				activeStream = streams.Current;
				return true;
			} else {
				activeStream = Null;
				return false;
			}
		}

		protected override void Dispose(bool disposing) {
			if (disposing) {
				activeStream.Dispose();
				streams.Dispose();
			}
		}

		public override void Flush() { }

		public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
		public override void SetLength(long value) => throw new NotSupportedException();
		public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
	}

}
