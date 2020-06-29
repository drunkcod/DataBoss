using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Cone;
using DataBoss.Data;
using DataBoss.Data.Common;
using DataBoss.DataPackage;

namespace DataBoss.Specs.DataPackage
{
	[Describe(typeof(CsvDataReader))]
	public class CsvDataReaderSpec
	{
		public void field_nullability_defaults_to_true() {
			var csv = new CsvDataReader(
				new CsvHelper.CsvReader(TextReader.Null),
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

		public void required_field() {
			var csv = new CsvDataReader(
				new CsvHelper.CsvReader(TextReader.Null),
				new TabularDataSchema {
					Fields = new List<TabularDataSchemaFieldDescription> {
						new TabularDataSchemaFieldDescription(
							"integer", 
							"integer", 
							new TabularDataSchemaFieldConstraints  {
								IsRequired = true,
							}),
					}
				}, hasHeaderRow: false);

			var schema = ObjectReader.For(csv.GetSchemaTable().CreateDataReader()).Read<DataReaderSchemaRow>().ToList();
			Check.That(
				() => schema.Single(x => x.ColumnName == "integer").AllowDBNull == false);
		}

		public void identity_column_is_required() {
			var csv = new CsvDataReader(
				new CsvHelper.CsvReader(TextReader.Null),
				new TabularDataSchema {
					Fields = new List<TabularDataSchemaFieldDescription> {
						new TabularDataSchemaFieldDescription("id", "integer"),
					}, 
					PrimaryKey = new List<string>{ "id" },
				}, hasHeaderRow: false);

			var schema = ObjectReader.For(csv.GetSchemaTable().CreateDataReader()).Read<DataReaderSchemaRow>().ToList();
			Check.That(() => schema.Single(x => x.ColumnName == "id").AllowDBNull == false);
		}

		public void detect_missing_required_value() {
			var csv = new CsvDataReader(
				new CsvHelper.CsvReader(new StringReader("1,\n,\n")),
				new TabularDataSchema {
					Fields = new List<TabularDataSchemaFieldDescription> {
						new TabularDataSchemaFieldDescription(
							"Id",
							"integer",
							new TabularDataSchemaFieldConstraints  {
								IsRequired = true,
							}),
					}
				}, hasHeaderRow: false);

			Check.Exception<InvalidOperationException>(() => ObjectReader.For(csv).Read<IdRow<int>>().ToList());
		}

		public void support_varying_decimal_separator() {
			var csv = new CsvDataReader(
				new CsvHelper.CsvReader(new StringReader("3·1415")),
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