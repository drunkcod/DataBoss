using System;
using System.Data;

namespace DataBoss.Data
{
	public class DataReaderDecorator : IDataReader
	{
		IDataReader inner;

		public DataReaderDecorator(IDataReader inner) {
			this.inner = inner ?? throw new ArgumentNullException(nameof(inner));
			this.GetName = inner.GetName;
		}

		public Func<int, string> GetName; 
		public event Action<IDataRecord> RecordRead;
		public event Action Closed;

		public object this[int i] => inner[i];
		public object this[string name] => inner[name];

		public int Depth => inner.Depth;
		public int FieldCount => inner.FieldCount;
		public bool IsClosed => inner.IsClosed;
		public int RecordsAffected => inner.RecordsAffected;

		public void Close() {
			inner.Close();
			Closed?.Invoke();
		}

		public void Dispose() {
			if(inner == null)
				return;
			if(!IsClosed)
				Close();
			inner.Dispose();
			inner = null;
		}

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
		string IDataRecord.GetName(int i) => GetName(i);
		public int GetOrdinal(string name) => inner.GetOrdinal(name);
		public DataTable GetSchemaTable() => inner.GetSchemaTable();
		public string GetString(int i) => inner.GetString(i);
		public object GetValue(int i) => inner.GetValue(i);
		public int GetValues(object[] values) => inner.GetValues(values);
		public bool IsDBNull(int i) => inner.IsDBNull(i);

		public bool NextResult() => inner.NextResult();

		public bool Read() {
			if(inner.Read()) {
				RecordRead?.Invoke(this);
				return true;
			}
			return false;
		}
	}
}
