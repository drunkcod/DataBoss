using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;

namespace DataBoss.Data
{
	public class DataReaderSchemaTable : IEnumerable<DataReaderSchemaRow>
	{		
		readonly List<DataReaderSchemaRow> rows = new List<DataReaderSchemaRow>();

		public int Count => rows.Count;

		public DataReaderSchemaRow this[int index] => rows[index];

		public void Add(string name, int ordinal, Type columnType, bool allowDBNull, int? columnSize = null, string dataTypeName = null) {
			rows.Add(new DataReaderSchemaRow {
				ColumnName = name,
				Ordinal = ordinal,
				ColumnType = columnType,
				AllowDBNull = allowDBNull,
				ColumnSize = columnSize,
				DataTypeName = dataTypeName,
			});
		}

		public int GetOrdinal(string name) {
			var n = rows.FindIndex(x => x.ColumnName == name);
			return n != -1 ? n : throw new InvalidOperationException($"No column named '{name}' found");
		}

		public DataTable ToDataTable() {
			var schema = new DataTable();
			var columnName = schema.Columns.Add(DataReaderSchemaColumns.ColumnName);
			var columnOrdinal = schema.Columns.Add(DataReaderSchemaColumns.ColumnOrdinal);
			var columnSize = schema.Columns.Add(DataReaderSchemaColumns.ColumnSize);
			var allowDBNull = schema.Columns.Add(DataReaderSchemaColumns.AllowDBNull);
			var dataType = schema.Columns.Add(DataReaderSchemaColumns.DataType);
			var dataTypeName = schema.Columns.Add(DataReaderSchemaColumns.DataTypeName);
			var isKey = schema.Columns.Add(DataReaderSchemaColumns.IsKey);
			foreach(var item in rows) {
				var r = schema.NewRow();
				r[columnName] = item.ColumnName;
				r[columnOrdinal] = item.Ordinal;
				r[columnSize] = item.ColumnSize.HasValue ? (object)item.ColumnSize.Value : DBNull.Value;
				r[allowDBNull] = item.AllowDBNull;
				r[dataType] = item.ColumnType;
				r[dataTypeName] = item.DataTypeName;
				r[isKey] = false;
				schema.Rows.Add(r);
			}

			return schema;
		}

		public IEnumerator<DataReaderSchemaRow> GetEnumerator() => rows.GetEnumerator();
		IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
	}
}
