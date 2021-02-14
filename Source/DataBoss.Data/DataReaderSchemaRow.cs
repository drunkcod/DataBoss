using System;

namespace DataBoss.Data
{
	public class DataReaderSchemaRow
	{
		Type columnType;

		public string ColumnName;
		public int Ordinal;
		public Type ColumnType 
		{
			get => columnType;
			set {
				this.columnType = value;
				this.IsValueType = value.IsValueType;
			}
		}
		public bool IsValueType { get; private set; }
		public bool AllowDBNull;
		public int? ColumnSize;
		public string DataTypeName;

		/// <summary>
		/// If this was a field what would type be then? 
		/// When nulls are allowed it's a Nullable<T> rather than T 
		public Type GetFieldType() => 
			ColumnType.IsPrimitive && AllowDBNull ? typeof(Nullable<>).MakeGenericType(ColumnType) : ColumnType;
	}
}
