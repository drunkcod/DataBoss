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
		public string DataTypeName;

		/// <summary>
		/// If this was a field what would type be then? 
		/// If nulls are allowed it's a Nullable<T> rather than T 
		public Type GetFieldType() => 
			ColumnType.IsPrimitive && AllowDBNull ? typeof(Nullable<>).MakeGenericType(ColumnType) : ColumnType;
	}
}
