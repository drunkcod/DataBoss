using System;
using System.Collections.Generic;
using System.Data;
using System.Reflection;

namespace DataBoss.Data
{
	public static class SequenceDataReader
	{
		public static SequenceDataReader<T> Create<T>(IEnumerable<T> data, Action<FieldMapping<T>> mapFields) {
			var fieldMapping = new FieldMapping<T>();
			mapFields(fieldMapping);
			return new SequenceDataReader<T>(data.GetEnumerator(), fieldMapping);
		}

		public static SequenceDataReader<T> Create<T>(IEnumerable<T> data, params string[] members) {
			return Create(data, fields => Array.ForEach(members, x => fields.Map(x)));
		}

		public static SequenceDataReader<T> Create<T>(IEnumerable<T> data, params MemberInfo[] members) {
			return Create(data, fields => Array.ForEach(members, x => fields.Map(x)));
		}
	}

	public class SequenceDataReader<T> : IDataReader
	{
		readonly object[] current;
		readonly Action<T,object[]> accessor;
		readonly IEnumerator<T> data;
		readonly string[] fieldNames;
	
		public SequenceDataReader(IEnumerator<T> data, FieldMapping<T> fields) {
			this.data = data;
			this.fieldNames = fields.GetFieldNames();
			this.accessor = fields.GetAccessor();
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

		public string GetName(int i) { return fieldNames[i]; }
		public int GetOrdinal(string name) {
			for(var i = 0; i != fieldNames.Length; ++i)
				if(fieldNames[i] == name)
					return i;
			throw new InvalidOperationException($"No field named '{name}' mapped");
		}

		public object GetValue(int i) => current[i];
		//SqlBulkCopy.EnableStreaming requires this
		public bool IsDBNull(int i) => GetValue(i) is DBNull;

	#region Here Be Dragons (not implemented / supported)
		int IDataReader.Depth { get { throw new NotSupportedException(); } }
		bool IDataReader.IsClosed { get { throw new NotSupportedException(); } }
		int IDataReader.RecordsAffected { get { throw new NotSupportedException(); } }
		DataTable IDataReader.GetSchemaTable() { throw new NotSupportedException(); }

		public string GetDataTypeName(int i) { throw new NotImplementedException(); }
		public Type GetFieldType(int i) { throw new NotImplementedException(); }
		public int GetValues(object[] values) { throw new NotImplementedException(); }
		public bool GetBoolean(int i) { throw new NotImplementedException(); }
		public byte GetByte(int i) { throw new NotImplementedException(); }
		public long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length) { throw new NotImplementedException(); }
		public char GetChar(int i) { throw new NotImplementedException(); }
		public long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length) { throw new NotImplementedException(); }
		public Guid GetGuid(int i) { throw new NotImplementedException(); }
		public short GetInt16(int i) { throw new NotImplementedException(); }
		public int GetInt32(int i) { throw new NotImplementedException(); }
		public long GetInt64(int i) { throw new NotImplementedException(); }
		public float GetFloat(int i) { throw new NotImplementedException(); }
		public double GetDouble(int i) { throw new NotImplementedException(); }
		public string GetString(int i) { throw new NotImplementedException(); }
		public decimal GetDecimal(int i) { throw new NotImplementedException(); }
		public DateTime GetDateTime(int i) { throw new NotImplementedException(); }
		public IDataReader GetData(int i) { throw new NotImplementedException(); }
	#endregion
	}
}
