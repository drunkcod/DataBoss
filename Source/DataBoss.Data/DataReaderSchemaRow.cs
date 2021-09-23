using System;

namespace DataBoss.Data
{
	public class DataReaderSchemaRow
	{
		Type dataType;

		public string ColumnName;
		public int Ordinal;
		public Type DataType 
		{
			get => dataType;
			set {
				this.dataType = value;
				this.IsValueType = value.IsValueType;
			}
		}
		public bool IsValueType { get; private set; }
		public bool AllowDBNull;
		public int? ColumnSize;
		public string DataTypeName;

		public Type ProviderSpecificDataType;

		/// <summary>
		/// If this was a field what would type be then? 
		/// When nulls are allowed it's a Nullable<T> rather than T 
		public Type GetFieldType() => 
			DataType.IsPrimitive && AllowDBNull ? typeof(Nullable<>).MakeGenericType(DataType) : DataType;
	}
}
