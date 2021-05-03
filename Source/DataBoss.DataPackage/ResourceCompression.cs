using System;
using System.IO;
using System.IO.Compression;

namespace DataBoss.DataPackage
{
	public class ResourceCompression
	{
		public static readonly ResourceCompression None = NoResourceCompression();

		public static ResourceCompression NoResourceCompression(CompressionLevel archiveCompression = CompressionLevel.Optimal) => new(
			archiveCompression,
			CompressionLevel.NoCompression,
			string.Empty,
			x => x,
			(x, _) => x);

		public static ResourceCompression GZip = new(
			CompressionLevel.NoCompression, 
			CompressionLevel.Optimal,
			".gz", 
			x => new GZipStream(x, CompressionMode.Decompress),
			(x, level) => new GZipStream(x, level));

#if NETSTANDARD2_1
		public static ResourceCompression Brotli = new(
			CompressionLevel.NoCompression, 
			(CompressionLevel)7,
			".br", 
			x => new BrotliStream(x, CompressionMode.Decompress),
			(x, level) => new BrotliStream(x, level));
#else
		public static ResourceCompression Brotli = new(
			CompressionLevel.NoCompression, 
			(CompressionLevel)7,
			".br", 
			NoBrotliSupport,
			(x, level) => NoBrotliSupport(x));

		static Stream NoBrotliSupport(Stream s) => 
			throw new NotSupportedException("Brotli requires netstandard2.1+");
#endif

		readonly Func<Stream, CompressionLevel, Stream> wrapWrite;

		ResourceCompression(
			CompressionLevel archiveCompression, 
			CompressionLevel resourceCompression,
			string ext, 
			Func<Stream, Stream> wrapRead,
			Func<Stream, CompressionLevel, Stream> wrapWrite) {
			this.ArchiveCompressionLevel = archiveCompression;
			this.ResourceCompressionLevel = resourceCompression;
			this.ExtensionSuffix = ext;
			this.WrapRead = wrapRead;
			this.wrapWrite = wrapWrite;
		}

		public ResourceCompression WithCompressionLevel(CompressionLevel compressionLevel) =>
			new ResourceCompression(ArchiveCompressionLevel, compressionLevel, ExtensionSuffix, WrapRead, wrapWrite);

		public static Stream OpenRead(string path, Func<string, Stream> open) {
			var r = open(path);
			var ext = Path.GetExtension(path);
			foreach (var item in new[] { GZip, Brotli })
				if (item.ExtensionSuffix == ext)
					return item.WrapRead(r);
			return r;
		}

		public bool TryGetOutputPath(ResourcePath path, out string outputPath) {
			if (!path.TryGetOutputPath(out outputPath))
				return false;

			if (string.IsNullOrEmpty(ExtensionSuffix))
				return true;

			outputPath = Path.ChangeExtension(outputPath, Path.GetExtension(outputPath) + ExtensionSuffix);
			return true;
		}

		public readonly CompressionLevel ArchiveCompressionLevel;
		public readonly CompressionLevel ResourceCompressionLevel;
		public readonly string ExtensionSuffix;
		public readonly Func<Stream, Stream> WrapRead;
		public Stream WrapWrite(Stream stream) => wrapWrite(stream, ResourceCompressionLevel);

	}
}
