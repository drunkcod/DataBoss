using System;
using System.Collections.Generic;
using System.Data;
using DataBoss.Data;
using DataBoss.DataPackage.Schema;

namespace DataBoss.DataPackage
{
	public class CsvResourceBuilder 
	{
		readonly CsvResourceOptions options = new();
		readonly CsvDialectDescription dialect = CsvDialectDescription.GetDefaultDialect();
		readonly TabularDataSchema schema = new() {
			PrimaryKey = new List<string>(),
			ForeignKeys = new List<DataPackageForeignKey>(),
		};

		Func<IDataReader> getData;

		public CsvResourceBuilder WithName(string name) {
			options.Name = name;
			return this;
		}

		public CsvResourceBuilder WithData(Func<IDataReader> getData) {
			this.getData = getData;
			return this;
		}

		public CsvResourceBuilder WithData<T>(IEnumerable<T> data) =>
			 WithData(BoundMethod.Bind(SequenceDataReader.ToDataReader, data));

		public CsvResourceBuilder WithData<T>(Func<IEnumerable<T>> getData) =>
			 WithData(BoundMethod.Bind(GetSequenceReader, getData));
		
		static IDataReader GetSequenceReader<T>(Func<IEnumerable<T>> getData) =>
			getData().ToDataReader();

		public CsvResourceBuilder WithDelimiter(string delimiter) {
			dialect.Delimiter = delimiter;
			return this;
		}

		public CsvResourceBuilder WithHeaderRow(bool includeHeader = true) {
			dialect.HasHeaderRow = includeHeader;
			return this;
		}

		public CsvResourceBuilder WithoutHeaderRow() =>
			WithHeaderRow(includeHeader: false);

		public CsvResourceBuilder WithPrimaryKey(params string[] parts) {
			if (parts != null && parts.Length > 0)
				schema.PrimaryKey.AddRange(parts);
			return this;
		}

		public CsvResourceBuilder WithForeignKey(string field, DataPackageKeyReference reference) =>
			WithForeignKey(new DataPackageForeignKey(field, reference));

		public CsvResourceBuilder WithForeignKey(DataPackageForeignKey fk) {
			schema.ForeignKeys.Add(fk);
			return this;
		}

		public TabularDataResource Build() =>
			TabularDataResource.From(
				new DataPackageResourceDescription {
					Format = "csv",
					Name = options.Name ?? throw new InvalidOperationException("Missing Name, call WithName."),
					Path = options.Path,
					Schema = schema.Clone(),
					Dialect = dialect,
				}, getData ?? throw new InvalidOperationException("No data available, call WithData."));
	}
}
