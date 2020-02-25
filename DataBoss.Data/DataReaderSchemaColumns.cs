using System;
using System.Data;

namespace DataBoss.Data
{
	public static class DataReaderSchemaColumns
	{
		public struct SchemaColumn
		{
			public readonly string Name;
			public readonly Type ColumnType;

			public SchemaColumn(string name, Type columnType) {
				this.Name = name;
				this.ColumnType = columnType;
			}
		}

		public static readonly SchemaColumn AllowDBNull = new SchemaColumn("AllowDBNull", typeof(bool));
		public static readonly SchemaColumn ColumnName = new SchemaColumn("ColumnName", typeof(string));
		public static readonly SchemaColumn ColumnOrdinal = new SchemaColumn("ColumnOrdinal", typeof(int));
		public static readonly SchemaColumn ColumnSize = new SchemaColumn("ColumnSize", typeof(int));
		public static readonly SchemaColumn DataType = new SchemaColumn("DataType", typeof(Type));
		public static readonly SchemaColumn DataTypeName = new SchemaColumn("DataTypeName", typeof(string));

		public static DataColumn Add(this DataColumnCollection cs, SchemaColumn column) => cs.Add(column.Name, column.ColumnType);
	}
}
