using CheckThat;
using Xunit;

namespace DataBoss.Data
{
	public class DataReaderSchemaTableTests
	{
		[Fact]
		public void DataReaderSchemaTable_for_null_column_size() {
			var sourceSchema = new DataReaderSchemaTable();
			sourceSchema.Add("Column", 0, typeof(int), true, columnSize: null);

			var schema = DataReaderExtensions.GetDataReaderSchemaTable(sourceSchema.ToDataTable());

			Check.That(
				() => schema[0].ColumnSize == sourceSchema[0].ColumnSize,
				() => schema[0].ColumnName == sourceSchema[0].ColumnName);
		}

		[Fact]
		public void sorts_rows_by_ordinal() {
			var schema = new DataReaderSchemaTable {
				{ "Second", 1, typeof(int), false },
				{ "First", 0, typeof(int), false }
			};

			Check.That(
				() => schema[0].Ordinal == 0, 
				() => schema[1].Ordinal == 1);
		}

	}
}
