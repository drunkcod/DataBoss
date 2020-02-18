using System;
using System.Collections.Generic;
using System.Data;
using System.Reflection;

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

		public static DataColumn Add(this DataColumnCollection cs, SchemaColumn column) => cs.Add(column.Name, column.ColumnType);
	}

	public class DataReaderSchemaRow
	{
		public string ColumnName;
		public int Ordinal;
		public Type ColumnType;
		public bool AllowDBNull;
		public int? ColumnSize;
	}

	public class DataReaderSchemaTable
	{		
		readonly List<DataReaderSchemaRow> rows = new List<DataReaderSchemaRow>();

		public int Count => rows.Count;

		public DataReaderSchemaRow this[int index] => rows[index];

		public void Add(string name, int ordinal, Type columnType, bool allowDBNull, int? columnSize = null) {
			rows.Add(new DataReaderSchemaRow {
				ColumnName = name,
				Ordinal = ordinal,
				ColumnType = columnType,
				AllowDBNull = allowDBNull,
				ColumnSize = columnSize,
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
			foreach(var item in rows) {
				var r = schema.NewRow();
				r[columnName] = item.ColumnName;
				r[columnOrdinal] = item.Ordinal;
				r[columnSize] = item.ColumnSize.HasValue ? (object)item.ColumnSize.Value : DBNull.Value;
				r[allowDBNull] = item.AllowDBNull;
				r[dataType] = item.ColumnType;
				schema.Rows.Add(r);
			}

			return schema;
		}
	}

	public static class SequenceDataReader
	{
		public static IDataReader Create<T>(params T[] data) => Create((IEnumerable<T>)data);
		public static IDataReader Create<T>(IEnumerable<T> data) => Create(data, x => x.MapAll());
		public static IDataReader Create<T>(IEnumerable<T> data, Action<FieldMapping<T>> mapFields) {
			var fieldMapping = new FieldMapping<T>();
			mapFields(fieldMapping);
			return new SequenceDataReader<T>(data?.GetEnumerator(), fieldMapping);
		}

		public static IDataReader Create<T>(IEnumerable<T> data, params string[] members) =>
			Create(data, fields => Array.ForEach(members, x => fields.Map(x)));

		public static IDataReader Create<T>(IEnumerable<T> data, params MemberInfo[] members) =>
			Create(data, fields => Array.ForEach(members, x => fields.Map(x)));

		public static IDataReader ToDataReader<T>(this IEnumerable<T> data) => Create(data); 
	}

	public class SequenceDataReader<T> : IDataReader
	{
		readonly object[] current;
		readonly Action<T,object[]> accessor;
		readonly IEnumerator<T> data;
		readonly string[] fieldNames;
		readonly Type[] fieldTypes;
		readonly DataBossDbType[] dbTypes;
	
		internal SequenceDataReader(IEnumerator<T> data, FieldMapping<T> fields) {
			this.data = data ?? throw new ArgumentNullException(nameof(data));
			this.fieldNames = fields.GetFieldNames();
			this.fieldTypes = fields.GetFieldTypes();
			this.accessor = fields.GetAccessor();
			this.dbTypes = fields.GetDbTypes();
			this.current = new object[fieldNames.Length];
		}

		public object this[int i] => GetValue(i);
		public object this[string name] => GetValue(GetOrdinal(name));

		public int FieldCount => current.Length;
	
		public bool Read() { 
			if(!data.MoveNext())
				return false;
			accessor(data.Current, current);
			return true;
		}

		public bool NextResult() => false;

		public void Close() { }

		public void Dispose() => data.Dispose();

		public string GetName(int i) => fieldNames[i];
		public Type GetFieldType(int i) => fieldTypes[i];
		public string GetDataTypeName(int i) => dbTypes[i].TypeName;
		public int GetOrdinal(string name) {
			for(var i = 0; i != fieldNames.Length; ++i)
				if(fieldNames[i] == name)
					return i;
			throw new InvalidOperationException($"No field named '{name}' mapped");
		}

		public object GetValue(int i) => current[i];

		public int GetValues(object[] values) {
			var n = Math.Min(current.Length, values.Length);
			Array.Copy(current, values, n);
			return n;
		}

		DataTable IDataReader.GetSchemaTable() {
			var schema = new DataReaderSchemaTable();
			for(var i = 0; i != FieldCount; ++i)
				schema.Add(fieldNames[i], i, GetFieldType(i), dbTypes[i].IsNullable, dbTypes[i].ColumnSize);
			return schema.ToDataTable();
		}

		//SqlBulkCopy.EnableStreaming requires this
		public bool IsDBNull(int i) {
			var x = GetValue(i);
			return (GetFieldType(i).IsValueType == false && x == null) || x is DBNull;
		}

		int IDataReader.Depth => throw new NotSupportedException();
		bool IDataReader.IsClosed => throw new NotSupportedException();
		int IDataReader.RecordsAffected => throw new NotSupportedException();

		public bool GetBoolean(int i) => (bool)GetValue(i);
		public byte GetByte(int i) => (byte)GetValue(i);
		public long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length) => throw new NotImplementedException();
		public char GetChar(int i) => (char)GetValue(i);
		public long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length) => throw new NotImplementedException();
		public Guid GetGuid(int i) => (Guid)GetValue(i);
		public short GetInt16(int i) => (short)GetValue(i);
		public int GetInt32(int i) => (int)GetValue(i);
		public long GetInt64(int i) => (long)GetValue(i);
		public float GetFloat(int i) => (float)GetValue(i);
		public double GetDouble(int i) => (double)GetValue(i);
		public string GetString(int i) => (string)GetValue(i);
		public decimal GetDecimal(int i) => (decimal)GetValue(i);
		public DateTime GetDateTime(int i) => (DateTime)GetValue(i);
		
		public IDataReader GetData(int i) => throw new NotImplementedException();
	}
}
