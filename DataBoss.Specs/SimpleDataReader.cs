using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using Cone.Core;

namespace DataBoss.Specs
{
	class SimpleDataReader : IDataReader, IEnumerable<object[]>
	{
		readonly string[] names;			
		readonly List<object[]> records = new List<object[]>();
		int currentRecord;

		public SimpleDataReader(params string[] names) {
			this.names = names;
		}

		public void Add(params object[] record) {
			if(record.Length != names.Length)
				throw new InvalidOperationException("Invalid record length");
			records.Add(record);
		}

		public int Count => records.Count;
		public int FieldCount => names.Length;

		public bool Read() {
			if(currentRecord == records.Count)
				return false;
			++currentRecord;
			return true;
		}
		public string GetName(int i) { return names[i]; }
		public object GetValue(int i) { return records[currentRecord - 1][i]; }

		public void Dispose() { }

		public string GetDataTypeName(int i) { throw new NotImplementedException(); }
		public Type GetFieldType(int i) { throw new NotImplementedException(); }
		public int GetValues(object[] values) { throw new NotImplementedException(); }
		public int GetOrdinal(string name) => names.IndexOf(name);
	
		public long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length) { throw new NotImplementedException(); }
		public long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length) { throw new NotImplementedException(); }

		public bool GetBoolean(int i) => (bool)GetValue(i);
		public byte GetByte(int i) => (byte)GetValue(i);
		public char GetChar(int i) => (char)GetValue(i);
		public Guid GetGuid(int i) => (Guid)GetValue(i);
		public short GetInt16(int i) => (short)GetValue(i);
		public int GetInt32(int i) => (int)GetValue(i);
		public long GetInt64(int i) => (long)GetValue(i);
		public float GetFloat(int i) => (float)GetValue(i);
		public double GetDouble(int i) => (double)GetValue(i);
		public string GetString(int i) => (string)GetValue(i);
		public decimal GetDecimal(int i) => (decimal)GetValue(i);
		public DateTime GetDateTime(int i) => (DateTime)GetValue(i);
		public IDataReader GetData(int i) => (IDataReader)GetValue(i);
		public bool IsDBNull(int i) => GetValue(i) == null;

		object IDataRecord.this[int i] => records[i];
		object IDataRecord.this[string name] => GetValue(GetOrdinal(name));

		public void Close() { }

		public DataTable GetSchemaTable() { throw new NotImplementedException(); }

		public bool NextResult() => false;

		public int Depth { get { throw new NotImplementedException(); } }
		public bool IsClosed { get { throw new NotImplementedException(); } }
		public int RecordsAffected { get { throw new NotImplementedException(); } }

		IEnumerator<object[]> IEnumerable<object[]>.GetEnumerator() => records.GetEnumerator();
		IEnumerator IEnumerable.GetEnumerator() => ((IEnumerable) records).GetEnumerator();
	}
}