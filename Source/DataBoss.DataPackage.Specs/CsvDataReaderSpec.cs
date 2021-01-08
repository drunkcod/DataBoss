using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using CheckThat;
using DataBoss.Data;
using DataBoss.Data.Common;
using Xunit;

namespace DataBoss.DataPackage
{
	public class CsvDataReaderSpec
	{
		[Fact]
		public void field_nullability_defaults_to_true() {
			var csv = new CsvDataReader(
				new CsvHelper.CsvReader(TextReader.Null, CultureInfo.CurrentCulture),
				new TabularDataSchema {
					Fields = new List<TabularDataSchemaFieldDescription> {
						new TabularDataSchemaFieldDescription("boolean", "boolean"),
						new TabularDataSchemaFieldDescription("datetime", "datetime"),
						new TabularDataSchemaFieldDescription("integer", "integer"),
						new TabularDataSchemaFieldDescription("number", "number"),
					}
				}, hasHeaderRow: false);

			var schema = ObjectReader.For(csv.GetSchemaTable().CreateDataReader()).Read<DataReaderSchemaRow>().ToList();
			Check.That(
				() => schema.Single(x => x.ColumnName == "boolean").AllowDBNull == true,
				() => schema.Single(x => x.ColumnName == "datetime").AllowDBNull == true,
				() => schema.Single(x => x.ColumnName == "integer").AllowDBNull == true,
				() => schema.Single(x => x.ColumnName == "number").AllowDBNull == true);
		}

		[Fact]
		public void required_field() {
			var csv = new CsvDataReader(
				new CsvHelper.CsvReader(TextReader.Null, CultureInfo.CurrentCulture),
				new TabularDataSchema {
					Fields = new List<TabularDataSchemaFieldDescription> {
						new TabularDataSchemaFieldDescription(
							"integer", 
							"integer", 
							constraints: new TabularDataSchemaFieldConstraints  {
								IsRequired = true,
							}),
					}
				}, hasHeaderRow: false);

			var schema = ObjectReader.For(csv.GetSchemaTable().CreateDataReader()).Read<DataReaderSchemaRow>().ToList();
			Check.That(
				() => schema.Single(x => x.ColumnName == "integer").AllowDBNull == false);
		}

		[Fact]
		public void identity_column_is_required() {
			var csv = new CsvDataReader(
				new CsvHelper.CsvReader(TextReader.Null, CultureInfo.CurrentCulture),
				new TabularDataSchema {
					Fields = new List<TabularDataSchemaFieldDescription> {
						new TabularDataSchemaFieldDescription("id", "integer"),
					}, 
					PrimaryKey = new List<string>{ "id" },
				}, hasHeaderRow: false);

			var schema = ObjectReader.For(csv.GetSchemaTable().CreateDataReader()).Read<DataReaderSchemaRow>().ToList();
			Check.That(() => schema.Single(x => x.ColumnName == "id").AllowDBNull == false);
		}

		[Fact]
		public void detect_missing_required_value() {
			var csv = new CsvDataReader(
				new CsvHelper.CsvReader(new StringReader("1,\n,\n"), CultureInfo.CurrentCulture),
				new TabularDataSchema {
					Fields = new List<TabularDataSchemaFieldDescription> {
						new TabularDataSchemaFieldDescription(
							"Id",
							"integer",
							constraints: new TabularDataSchemaFieldConstraints  {
								IsRequired = true,
							}),
					}
				}, hasHeaderRow: false);

			Check.Exception<InvalidOperationException>(() => ObjectReader.For(csv).Read<IdRow<int>>().ToList());
		}

		[Fact]
		public void support_varying_decimal_separator() {
			var csv = new CsvDataReader(
				new CsvHelper.CsvReader(new StringReader("3·1415"), CultureInfo.CurrentCulture),
				new TabularDataSchema {
					Fields = new List<TabularDataSchemaFieldDescription> {
						new TabularDataSchemaFieldDescription(
							"value",
							"number",
							decimalChar: "·"//interpunct, no-one uses that.
						),
					}
				}, hasHeaderRow: false);

			Check.With(() => ObjectReader.For(csv).Read<ValueRow<double>>().ToList()).That(				
				xs => xs.Count == 1,
				xs => xs[0].Value == 3.1415);
		}

		class ValueRow<T> { public T Value { get; set; } }
	}
}
