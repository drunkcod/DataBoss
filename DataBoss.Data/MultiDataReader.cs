using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;

namespace DataBoss.Data
{
	public class MultiDataReader : IDataReader
	{ 
		readonly IDataReader[] readers;
		int next;
		int[] nextNext;
		IDataReader currentReader;
		public MultiDataReader(IEnumerable<IDataReader> readers) : this(readers.ToArray()) { }
		public MultiDataReader(params IDataReader[] readers) { 
			this.readers = readers;
			this.nextNext = new int[readers.Length];
			this.currentReader = readers[0];
			for(var i = 0; i != readers.Length; ++i)
				this.nextNext[i] = (i + 1) % readers.Length;
			this.next = readers.Length - 1;
		}

		public object this[int i] => currentReader[i];
		public object this[string name] => currentReader[name];
		public int FieldCount => currentReader.FieldCount;

		public int Depth => throw new NotImplementedException();

		public bool IsClosed => Array.TrueForAll(readers, x => x.IsClosed);

		public int RecordsAffected => throw new NotImplementedException();

		public void Close() => Array.ForEach(readers, x => x.Close());
		public void Dispose() => Array.ForEach(readers, x => x.Dispose());

		public bool GetBoolean(int i) => currentReader.GetBoolean(i);
		public byte GetByte(int i) => currentReader.GetByte(i);
		public long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length) => currentReader.GetBytes(i, fieldOffset, buffer, bufferoffset, length);
		public char GetChar(int i) => currentReader.GetChar(i);
		public long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length) => currentReader.GetChars(i, fieldoffset, buffer, bufferoffset, length);
		public IDataReader GetData(int i) => currentReader.GetData(i);
		public string GetDataTypeName(int i) => currentReader.GetDataTypeName(i);
		public DateTime GetDateTime(int i) => currentReader.GetDateTime(i);
		public decimal GetDecimal(int i) => currentReader.GetDecimal(i);
		public double GetDouble(int i) => currentReader.GetDouble(i);
		public Type GetFieldType(int i) => currentReader.GetFieldType(i);
		public float GetFloat(int i) => currentReader.GetFloat(i);
		public Guid GetGuid(int i) => currentReader.GetGuid(i);
		public short GetInt16(int i) => currentReader.GetInt16(i);
		public int GetInt32(int i) => currentReader.GetInt32(i);
		public long GetInt64(int i) => currentReader.GetInt64(i);
		public string GetName(int i) => currentReader.GetName(i);
		public int GetOrdinal(string name) => currentReader.GetOrdinal(name);
		public DataTable GetSchemaTable() => currentReader.GetSchemaTable();
		public string GetString(int i) => currentReader.GetString(i);
		public object GetValue(int i) => currentReader.GetValue(i);
		public int GetValues(object[] values) => currentReader.GetValues(values);
		public bool IsDBNull(int i) => currentReader.IsDBNull(i);

		public bool NextResult() { throw new NotSupportedException(); }

		public bool Read() {
			for(;;) {
				var current = nextNext[next];
				if (readers[current].Read()) {
					currentReader = readers[current];
					next = current;
					return true;
				} else {
					if(current == nextNext[current])
						return false;
					nextNext[next] = nextNext[current];
				}
			}
		}
	}
}
 