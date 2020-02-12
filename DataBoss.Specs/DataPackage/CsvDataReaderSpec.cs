using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Cone;
using DataBoss.Data;
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

			var schema = ObjectReader.For(csv.GetSchemaTable().CreateDataReader()).Read<SchemaTableRow>().ToList();
			Check.That(
				() => schema.Single(x => x.ColumnName == "boolean").AllowDbNull == true,
				() => schema.Single(x => x.ColumnName == "datetime").AllowDbNull == true,
				() => schema.Single(x => x.ColumnName == "integer").AllowDbNull == true,
				() => schema.Single(x => x.ColumnName == "number").AllowDbNull == true);
		}

		class SchemaTableRow
		{
			public string ColumnName;
			public bool AllowDbNull;
		}
	}
}
