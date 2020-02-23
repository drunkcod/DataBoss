using System;

namespace DataBoss.Data
{
	public class DataReaderSchemaRow
	{
		public string ColumnName;
		public int Ordinal;
		public Type ColumnType;
		public bool AllowDBNull;
		public int? ColumnSize;
	}
}
