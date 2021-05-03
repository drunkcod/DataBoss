using System;
using System.Data;

namespace DataBoss.Data
{
	class WhereDataReader : IDataReader
	{
		readonly IDataReader inner;
		readonly Func<IDataRecord, bool> predicate;

		public WhereDataReader(IDataReader inner, Func<IDataRecord, bool> predicate) {
			this.inner = inner;
			this.predicate = predicate;
		}

		public bool Read() {
			while(inner.Read())
				if(predicate(inner))
					return true;
			return false;
		}

		public object this[int i] => inner[i];
		public object this[string name] => inner[name];

		public int Depth => inner.Depth;
		public bool IsClosed => inner.IsClosed;

		public int RecordsAffected => inner.RecordsAffected;
		public int FieldCount => inner.FieldCount;

		public void Close() => inner.Close();
		public void Dispose() => inner.Dispose();

		public bool GetBoolean(int i) => inner.GetBoolean(i);
		public byte GetByte(int i) => inner.GetByte(i);
		public long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length) => inner.GetBytes(i, fieldOffset, buffer, bufferoffset, length);
		public char GetChar(int i) => inner.GetChar(i);
		public long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length) => inner.GetChars(i, fieldoffset, buffer, bufferoffset, length);
		public IDataReader GetData(int i) => inner.GetData(i);
		public string GetDataTypeName(int i) => inner.GetDataTypeName(i);
		public DateTime GetDateTime(int i) => inner.GetDateTime(i);
		public decimal GetDecimal(int i) => inner.GetDecimal(i);
		public double GetDouble(int i) => inner.GetDouble(i);
		public Type GetFieldType(int i) => inner.GetFieldType(i);
		public float GetFloat(int i) => inner.GetFloat(i);
		public Guid GetGuid(int i) => inner.GetGuid(i);
		public short GetInt16(int i) => inner.GetInt16(i);
		public int GetInt32(int i) => inner.GetInt32(i);
		public long GetInt64(int i) => inner.GetInt64(i);
		public string GetName(int i) => inner.GetName(i);
		public int GetOrdinal(string name) => inner.GetOrdinal(name);
		public DataTable GetSchemaTable() => inner.GetSchemaTable();
		public string GetString(int i) => inner.GetString(i);
		public object GetValue(int i) => inner.GetValue(i);
		public int GetValues(object[] values) => inner.GetValues(values);
		public bool IsDBNull(int i) => inner.IsDBNull(i);

		public bool NextResult() => inner.NextResult();
	}
}
