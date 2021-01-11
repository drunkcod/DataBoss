using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.IO;
using System.IO.Compression;
using System.Linq.Expressions;
using DataBoss.Data;

namespace DataBoss.DataPackage
{
	public interface IDataPackageBuilder
	{
		IDataPackageResourceBuilder AddResource(string name, Func<IDataReader> getData);
		void Save(Func<string, Stream> createOutput, CultureInfo culture = null);
		DataPackage Serialize(CultureInfo culture = null);
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
		public static IDataPackageResourceBuilder AddResource<T>(this IDataPackageBuilder self, string name, IEnumerable<T> data) =>
			self.AddResource(name, BoundMethod.Bind(SequenceDataReader.ToDataReader, data));

		public static IDataPackageResourceBuilder AddResource<T>(this IDataPackageBuilder self, string name, Func<IEnumerable<T>> getData) =>
			self.AddResource(name, BoundMethod.Bind(GetSequenceReader,  getData));

		static IDataReader GetSequenceReader<T>(Func<IEnumerable<T>> getData) =>
			getData().ToDataReader();

		public static IDataPackageResourceBuilder WithForeignKey(this IDataPackageResourceBuilder self, string field, DataPackageKeyReference reference) =>
			self.WithForeignKey(new DataPackageForeignKey(field, reference));

		public static void Save(this IDataPackageBuilder self, string path, CultureInfo culture = null) {
			Directory.CreateDirectory(path);
			self.Save(name => File.Create(Path.Combine(path, name)), culture);
		}

		public static void SaveZip(this IDataPackageBuilder self, string path, CultureInfo culture = null) => 
			SaveZip(self, File.Create(path, 16384), culture, leaveOpen: false);
		
		public static void SaveZip(this IDataPackageBuilder self, Stream stream, CultureInfo culture = null) =>
			SaveZip(self, stream, culture, leaveOpen: false);

		public static void SaveZip(this IDataPackageBuilder self, Stream stream, CultureInfo culture, bool leaveOpen) {
			if(!stream.CanSeek)//work around for ZipArchive Create mode reading Position.
				stream = new ZipOutputStream(stream);
			using (var zip = new ZipArchive(stream, ZipArchiveMode.Create, leaveOpen: leaveOpen))
				self.Save(x => {
					var e = zip.CreateEntry(x);
					SetExternalAttributes(e);
					return e.Open();
				}, culture);
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
