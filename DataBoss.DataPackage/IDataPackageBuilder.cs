using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.IO;
using System.IO.Compression;
using DataBoss.Data;

namespace DataBoss.DataPackage
{
	public interface IDataPackageBuilder
	{
		IDataPackageResourceBuilder AddResource(string name, Func<IDataReader> getData);
		void Save(Func<string, Stream> createOutput, CultureInfo culture = null);
	}

	public interface IDataPackageResourceBuilder : IDataPackageBuilder
	{
		IDataPackageResourceBuilder WithForeignKey(DataPackageForeignKey fk);
		IDataPackageResourceBuilder WithPrimaryKey(params string[] parts);
	}

	public static class DataPackageBuilderExtensions
	{
		public static IDataPackageResourceBuilder AddResource<T>(this IDataPackageBuilder self, string name, IEnumerable<T> data) =>
			self.AddResource(name, () => data.ToDataReader());

		public static IDataPackageResourceBuilder AddResource<T>(this IDataPackageBuilder self, string name, Func<IEnumerable<T>> getData) =>
			self.AddResource(name, () => getData().ToDataReader());

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
			using (var zip = new ZipArchive(stream, ZipArchiveMode.Create, leaveOpen: leaveOpen))
				self.Save(x => zip.CreateEntry(x).Open(), culture);
		}
	}
}
