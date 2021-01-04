using System;
using System.Collections.Generic;
using System.Data;
using System.Reflection;

namespace DataBoss.Data
{
	public static class SequenceDataReader
	{
		public static IDataReader Items<T>(params T[] data) => Create(data);
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
		readonly DataReaderSchemaTable schema = new DataReaderSchemaTable();
	
		internal SequenceDataReader(IEnumerator<T> data, FieldMapping<T> fields) {
			this.data = data ?? throw new ArgumentNullException(nameof(data));
			this.accessor = fields.GetAccessor();
			var fieldNames = fields.GetFieldNames();
			var fieldTypes = fields.GetFieldTypes();
			var dbTypes = fields.GetDbTypes();
			for(var i = 0; i != fieldNames.Length; ++i) 
				schema.Add(fieldNames[i], i, fieldTypes[i], dbTypes[i].IsNullable, columnSize: dbTypes[i].ColumnSize, dataTypeName: dbTypes[i].TypeName); 
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

		public string GetName(int i) => schema[i].ColumnName;
		public Type GetFieldType(int i) => schema[i].ColumnType;
		public string GetDataTypeName(int i) => schema[i].DataTypeName;
		public int GetOrdinal(string name) {
			var o = schema.GetOrdinal(name);
			if(o < 0)
				throw new InvalidOperationException($"No field named '{name}' mapped");
			return o;
		}

		public object GetValue(int i) => current[i];

		public int GetValues(object[] values) {
			var n = Math.Min(current.Length, values.Length);
			Array.Copy(current, values, n);
			return n;
		}

		DataTable IDataReader.GetSchemaTable() =>
			schema.ToDataTable();

		//SqlBulkCopy.EnableStreaming requires this
		public bool IsDBNull(int i) {
			var field = schema[i];
			if (!field.AllowDBNull)
				return false;
			var x = GetValue(i);
			return (field.IsValueType == false && x == null) || (DBNull.Value == x);
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
