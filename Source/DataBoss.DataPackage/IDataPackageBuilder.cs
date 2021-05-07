using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.IO;
using System.IO.Compression;
using System.Linq.Expressions;
using System.Threading.Tasks;
using DataBoss.Data;
using DataBoss.Threading;

namespace DataBoss.DataPackage
{
	public interface IDataPackageBuilder
	{
		[Obsolete("use \"AddResource(Action<CsvResourceBuilder> setupResource)\" instead.")]
		IDataPackageResourceBuilder AddResource(string name, Func<IDataReader> getData);
		IDataPackageBuilder AddResource(Action<CsvResourceBuilder> setupResource);

		void Save(Func<string, Stream> createOutput, DataPackageSaveOptions options);
		Task SaveAsync(Func<string, Stream> createOutput, DataPackageSaveOptions options);
		
		DataPackage Serialize(CultureInfo culture = null);
		Task<DataPackage> SerializeAsync(CultureInfo culture = null);
		DataPackage Done();
	}

	public interface IDataPackageResourceBuilder : IDataPackageBuilder
	{
		IDataPackageResourceBuilder WithForeignKey(DataPackageForeignKey fk);
		IDataPackageResourceBuilder WithPrimaryKey(params string[] parts);
		IDataPackageResourceBuilder WithDelimiter(string delimiter);
	}

	public static class DataPackageBuilderExtensions
	{
		[Obsolete("use \"AddResource(Action<CsvResourceBuilder> setupResource)\" instead.")]
		public static IDataPackageResourceBuilder AddResource<T>(this IDataPackageBuilder self, string name, IEnumerable<T> data) =>
			self.AddResource(name, BoundMethod.Bind(SequenceDataReader.ToDataReader, data));

		[Obsolete("use \"AddResource(Action<CsvResourceBuilder> setupResource)\" instead.")]
		public static IDataPackageResourceBuilder AddResource<T>(this IDataPackageBuilder self, string name, Func<IEnumerable<T>> getData) =>
			self.AddResource(name, BoundMethod.Bind(GetSequenceReader,  getData));

		static IDataReader GetSequenceReader<T>(Func<IEnumerable<T>> getData) =>
			getData().ToDataReader();

		public static IDataPackageResourceBuilder WithForeignKey(this IDataPackageResourceBuilder self, string field, DataPackageKeyReference reference) =>
			self.WithForeignKey(new DataPackageForeignKey(field, reference));

		public static void Save(this IDataPackageBuilder self, Func<string, Stream> createOutput, CultureInfo culture = null) =>
			self.Save(createOutput, new DataPackageSaveOptions { Culture = culture });

		public static void Save(this IDataPackageBuilder self, string path, CultureInfo culture = null) =>
			self.Save(name => File.Create(Path.Combine(path, name)), new DataPackageSaveOptions { Culture = culture });

		public static void Save(this IDataPackageBuilder self, string path, DataPackageSaveOptions options) {
			Directory.CreateDirectory(path);
			self.Save(name => File.Create(Path.Combine(path, name)), options);
		}

		public static void SaveZip(this IDataPackageBuilder self, string path, CultureInfo culture = null) => 
			SaveZip(self, File.Create(path, 16384), new DataPackageSaveOptions { Culture = culture }, leaveOpen: false);

		public static void SaveZip(this IDataPackageBuilder self, string path, DataPackageSaveOptions options) =>
			SaveZip(self, File.Create(path, 16384), options, leaveOpen: false);

		public static void SaveZip(this IDataPackageBuilder self, Stream stream, CultureInfo culture = null) =>
			SaveZip(self, stream, new DataPackageSaveOptions { Culture = culture }, leaveOpen: false);

		public static void SaveZip(this IDataPackageBuilder self, Stream stream, DataPackageSaveOptions options, bool leaveOpen = false) =>
			TaskRunner.Run(() => SaveZipAsync(self, stream, options, leaveOpen));

		public static Task SaveZipAsync(this IDataPackageBuilder self, Stream stream, CultureInfo culture = null) =>
			SaveZipAsync(self, stream, new DataPackageSaveOptions { Culture = culture }, leaveOpen: false);

		public static async Task SaveZipAsync(this IDataPackageBuilder self, Stream stream, DataPackageSaveOptions options, bool leaveOpen = false) {
			if(!stream.CanSeek)//work around for ZipArchive Create mode reading Position.
				stream = new ZipOutputStream(stream);

			using var zip = new ZipArchive(stream, ZipArchiveMode.Create, leaveOpen: leaveOpen);
			var compressionLevel = options.ResourceCompression.ArchiveCompressionLevel;

			await self.SaveAsync(x => {
				var e = zip.CreateEntry(x, compressionLevel);
				SetExternalAttributes(e);
				return e.Open();
			}, options);
		}

		static Action<ZipArchiveEntry> SetExternalAttributes = DetectExternalAttributeSupport;

		static void DetectExternalAttributeSupport(ZipArchiveEntry e) => (SetExternalAttributes = GetExternalAttributeSetter())(e);

		static Action<ZipArchiveEntry> GetExternalAttributeSetter() {
			var externalAttributes = typeof(ZipArchiveEntry).GetProperty("ExternalAttributes");
			if (externalAttributes == null)
				return DoNothing;

			var isFile = Convert.ToInt32("100000", 8);
			var chmod = Convert.ToInt32("664", 8);
			var p0 = Expression.Parameter(typeof(ZipArchiveEntry), "e");
			return Expression.Lambda<Action<ZipArchiveEntry>>(Expression.Assign(
					Expression.MakeMemberAccess(p0, externalAttributes), Expression.Constant((isFile | chmod) << 16)), p0).Compile();
		}

		static void DoNothing(ZipArchiveEntry _) { }
	}
}
