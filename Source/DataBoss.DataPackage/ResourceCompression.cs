using System;
using System.IO;
using System.IO.Compression;
using System.Linq.Expressions;
using DataBoss.IO;

namespace DataBoss.DataPackage
{

	public abstract class ResourceCompression
	{
		class StreamDecoroatorResourceCompression : ResourceCompression
		{
			readonly Func<Stream, CompressionLevel, Stream> wrapWrite;
			readonly Func<Stream, CompressionMode, Stream> open;
			readonly CompressionLevel archiveCompressionLevel;

			public StreamDecoroatorResourceCompression(
				CompressionLevel archiveCompression,
				CompressionLevel resourceCompression,
				string ext,
				Func<Stream, CompressionMode, Stream> open,
				Func<Stream, CompressionLevel, Stream> wrapWrite) {
					this.archiveCompressionLevel = archiveCompression;
					this.ResourceCompressionLevel = resourceCompression;
					this.ExtensionSuffix = ext;
					this.open = open;
					this.wrapWrite = wrapWrite;
				}

			public override ResourceCompression WithCompressionLevel(CompressionLevel compressionLevel) =>
				new StreamDecoroatorResourceCompression(ArchiveCompressionLevel, compressionLevel, ExtensionSuffix, open, wrapWrite);

			public override (string Path, Stream Stream) OpenWrite(string path, Func<string, Stream> createDestination) {
				var outputPath = GetOutputPath(path);
				return (outputPath, WrapWrite(createDestination(outputPath)));
			}

			protected override bool TryOpenRead(string path, Func<string, Stream> open, out Stream stream) {
				var ext = Path.GetExtension(path);
				if (ExtensionSuffix == ext) {
					stream = WrapRead(open(path));
					return true;
				}
				stream = null;
				return false;
			}

			string GetOutputPath(string path) {
				if (string.IsNullOrEmpty(ExtensionSuffix))
					return path;

				return Path.ChangeExtension(path, Path.GetExtension(path) + ExtensionSuffix);
			}

			public override CompressionLevel ArchiveCompressionLevel => archiveCompressionLevel;
			public readonly CompressionLevel ResourceCompressionLevel;
			public readonly string ExtensionSuffix;
			Stream WrapWrite(Stream stream) => wrapWrite(stream, ResourceCompressionLevel);
			Stream WrapRead(Stream stream) => open(stream, CompressionMode.Decompress);
		}

		class ZipResourceCompression : ResourceCompression
		{
			readonly CompressionLevel compressionLevel;

			public ZipResourceCompression(CompressionLevel compressionLevel) {
				this.compressionLevel = compressionLevel;
			}

			public override CompressionLevel ArchiveCompressionLevel => CompressionLevel.NoCompression;

			public override (string Path, Stream Stream) OpenWrite(string path, Func<string, Stream> createDestination) {
				var zipPath = Path.ChangeExtension(path, "zip");
				var zip = new ZipArchive(createDestination(zipPath), ZipArchiveMode.Create);
				var e = zip.CreateEntry(path, compressionLevel);
				var stream = new StreamDecorator(e.Open());
				stream.Closed += zip.Dispose;
				return (zipPath, stream);
			}

			protected override bool TryOpenRead(string path, Func<string, Stream> open, out Stream stream) {
				if(Path.GetExtension(path) != ".zip") {
					stream = null;
					return false;
				}

				var zip = new ZipArchive(open(path), ZipArchiveMode.Read);
				if (zip.Entries.Count != 1)
					throw new InvalidOperationException("Zip must contain exactly one entry.");
				var entry = new StreamDecorator(zip.Entries[0].Open());
				entry.Closed += zip.Dispose;
				stream = entry;
				return true;
			}

			public override ResourceCompression WithCompressionLevel(CompressionLevel compressionLevel) => 
				new ZipResourceCompression(compressionLevel);
		}

		public static readonly ResourceCompression None = new StreamDecoroatorResourceCompression(
			CompressionLevel.Optimal,
			CompressionLevel.NoCompression,
			string.Empty,
			(x, _) => x,
			(x, _) => x);

		public static ResourceCompression GZip = new StreamDecoroatorResourceCompression(
			CompressionLevel.NoCompression, 
			CompressionLevel.Optimal,
			".gz", 
			(x, mode) => new GZipStream(x, mode),
			(x, level) => new GZipStream(x, level));

		public static ResourceCompression Zip = new ZipResourceCompression(CompressionLevel.Optimal);

		public static ResourceCompression Brotli = BindBrotli();

		static ResourceCompression BindBrotli() {
			var (open, wrapWrite) = GetWrappers(
				typeName: "System.IO.Compression.BrotliStream, System.IO.Compression.Brotli",
				errorMessage: "Brotli requires netstandard2.1+");
			return new StreamDecoroatorResourceCompression(
				CompressionLevel.NoCompression,
				(CompressionLevel)7,
				".br",
				open,
				wrapWrite);
		}

		public abstract CompressionLevel ArchiveCompressionLevel { get; }

		static (Func<Stream, CompressionMode, Stream> Open, Func<Stream, CompressionLevel, Stream> WrapWrite) GetWrappers(string typeName, string errorMessage) {
			if (Type.GetType(typeName) is Type found)
				return (
					BindCtor<Func<Stream, CompressionMode, Stream>>(found),
					BindCtor<Func<Stream, CompressionLevel, Stream>>(found));
			return (
				delegate { throw new NotSupportedException(errorMessage); },
				delegate { throw new NotSupportedException(errorMessage); });
		}

		static TDelegate BindCtor<TDelegate>(Type type) where TDelegate : Delegate =>
			(TDelegate)MakeCtor(type, typeof(TDelegate)).Compile();
	
		static LambdaExpression MakeCtor(Type type, Type delegateType) {
			var args = Array.ConvertAll(
				delegateType.GetMethod("Invoke")?.GetParameters() ?? throw new InvalidOperationException("Invoke not found, non delegate type passed?."),
				x => Expression.Parameter(x.ParameterType));
			var ctor = type.GetConstructor(Array.ConvertAll(args, x => x.Type)) ?? throw new InvalidOperationException("No suitable ctor found.");
			return Expression.Lambda(Expression.New(ctor, args), tailCall: true, args);
		}

		public abstract ResourceCompression WithCompressionLevel(CompressionLevel compressionLevel);

		public static Stream OpenRead(string path, Func<string, Stream> open) {
			foreach (var item in new[] { GZip, Brotli, Zip })
				if (item.TryOpenRead(path, open, out var stream))
					return stream;

			return open(path);
		}

		protected virtual bool TryOpenRead(string path, Func<string, Stream> open, out Stream stream) {
			stream = null;
			return false;
		} 

		public abstract (string Path, Stream Stream) OpenWrite(string path, Func<string, Stream> createDestination);
	}
}
