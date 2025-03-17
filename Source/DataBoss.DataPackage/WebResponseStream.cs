using System.IO;
using System.Net;

namespace DataBoss.DataPackage
{
	class WebResponseStream : Stream
	{
		readonly WebResponse source;
		readonly Stream stream;

		public static WebResponseStream Get(string url) {
#pragma warning disable SYSLIB0014 // Type or member is obsolete
			var http = WebRequest.Create(url);
#pragma warning restore SYSLIB0014 // Type or member is obsolete
			http.Method = "GET";
			return new WebResponseStream(http.GetResponse());
		}

		WebResponseStream(WebResponse source) {
			this.source = source;
			this.stream = source.GetResponseStream();
		}

		public string ContentType => source.ContentType;

		protected override void Dispose(bool disposing) {
			if (!disposing)
				return;
			source.Dispose();
			stream.Dispose();
		}

		public override bool CanRead => stream.CanRead;
		public override bool CanSeek => stream.CanSeek;
		public override bool CanWrite => stream.CanWrite;

		public override long Length => stream.Length;
		public override long Position { get => stream.Position; set => stream.Position = value; }

		public override void Flush() => stream.Flush();

		public override int Read(byte[] buffer, int offset, int count) => stream.Read(buffer, offset, count);
		public override long Seek(long offset, SeekOrigin origin) => stream.Seek(offset, origin);
		public override void SetLength(long value) => stream.SetLength(value);
		public override void Write(byte[] buffer, int offset, int count) => stream.Write(buffer, offset, count);
	}
}
