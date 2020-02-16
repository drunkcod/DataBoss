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
						new TabularDataSchemaFieldDescription { Name = "boolean", Type = "boolean", },
						new TabularDataSchemaFieldDescription { Name = "datetime", Type = "datetime", },
						new TabularDataSchemaFieldDescription { Name = "integer", Type = "integer", },
						new TabularDataSchemaFieldDescription { Name = "number", Type = "number", },
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
						new TabularDataSchemaFieldDescription { 
							Name = "integer", 
							Type = "integer", 
							Constraints = new TabularDataSchemaFieldConstraints  {
								IsRequired = true,
							} 
						},
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
						new TabularDataSchemaFieldDescription {
							Name = "id",
							Type = "integer",
						},
					}, 
					PrimaryKey = new List<string>{ "id" },
				}, hasHeaderRow: false);

			var schema = ObjectReader.For(csv.GetSchemaTable().CreateDataReader()).Read<DataReaderSchemaRow>().ToList();
			Check.That(
				() => schema.Single(x => x.ColumnName == "id").AllowDBNull == false);
		}

		public void detect_missing_required_value() {
			var csv = new CsvDataReader(
				new CsvHelper.CsvReader(new StringReader("1,\n,\n")),
				new TabularDataSchema {
					Fields = new List<TabularDataSchemaFieldDescription> {
						new TabularDataSchemaFieldDescription {
							Name = "Id",
							Type = "integer",
							Constraints = new TabularDataSchemaFieldConstraints  {
								IsRequired = true,
							}
						},
								}
				}, hasHeaderRow: false);

			var e = Check.Exception<InvalidOperationException>(() => ObjectReader.For(csv).Read<IdRow<int>>().ToList());
			Check.That(() => e.InnerException.Message == "Unexpected null value.");
		}
	}
}
