using System;
using System.IO;
using System.IO.Compression;
using System.Linq.Expressions;

namespace DataBoss.DataPackage
{
	public class ResourceCompression
	{
		public static readonly ResourceCompression None = new(
			CompressionLevel.Optimal,
			CompressionLevel.NoCompression,
			string.Empty,
			(x, _) => x,
			(x, _) => x);

		public static ResourceCompression GZip = new(
			CompressionLevel.NoCompression, 
			CompressionLevel.Optimal,
			".gz", 
			(x, mode) => new GZipStream(x, mode),
			(x, level) => new GZipStream(x, level));

		public static ResourceCompression Brotli = BindBrotli();

		static ResourceCompression BindBrotli() {
			var (open, wrapWrite) = GetWrappers(
				typeName: "System.IO.Compression.BrotliStream, System.IO.Compression.Brotli",
				errorMessage: "Brotli requires netstandard2.1+");
			return new(
				CompressionLevel.NoCompression,
				(CompressionLevel)7,
				".br",
				open,
				wrapWrite);
		}

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
				return Expression.Lambda(
					Expression.New(ctor, args),
					tailCall: true,
					args);
			}

		readonly Func<Stream, CompressionLevel, Stream> wrapWrite;
		readonly Func<Stream, CompressionMode, Stream> open;

		ResourceCompression(
			CompressionLevel archiveCompression, 
			CompressionLevel resourceCompression,
			string ext, 
			Func<Stream, CompressionMode, Stream> open,
			Func<Stream, CompressionLevel, Stream> wrapWrite) {
			this.ArchiveCompressionLevel = archiveCompression;
			this.ResourceCompressionLevel = resourceCompression;
			this.ExtensionSuffix = ext;
			this.open = open;
			this.wrapWrite = wrapWrite;
		}

		public ResourceCompression WithCompressionLevel(CompressionLevel compressionLevel) =>
			new(ArchiveCompressionLevel, compressionLevel, ExtensionSuffix, open, wrapWrite);

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
		public Stream WrapWrite(Stream stream) => wrapWrite(stream, ResourceCompressionLevel);
		public Stream WrapRead(Stream stream) => open(stream, CompressionMode.Decompress);
	}
}
