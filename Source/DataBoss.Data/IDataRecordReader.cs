using System;
using System.Data;

namespace DataBoss.Data
{
	public interface IDataRecordReader 
	{
		bool Read();
		IDataRecord GetRecord();
	}

	public static class DataRecordReaderExtensions
	{
		public static IDataRecordReader AsDataRecordReader(this IDataReader reader) =>
			reader is IDataRecordReader records ? records : new ObjectDataRecordReader(reader);
	}

	class ObjectDataRecordReader : IDataRecordReader
	{
		readonly IDataReader reader;

		public ObjectDataRecordReader(IDataReader reader) {
			this.reader = reader;
		}

		public bool Read() => reader.Read();
		public IDataRecord GetRecord() => ObjectDataRecord.GetRecord(reader);
		public DataTable GetSchemaTable() => reader.GetSchemaTable();

		class ObjectDataRecord : IDataRecord
		{
			readonly object[] values;
			readonly int fieldCount;

			public static ObjectDataRecord GetRecord(IDataReader reader) {
				
				var fieldCount = reader.FieldCount;
				var fields = new object[fieldCount];

				for (var i = 0; i != fieldCount; ++i)
					fields[i] = reader.IsDBNull(i) ? DBNull.Value : reader.GetValue(i);

				return new ObjectDataRecord(fields, fieldCount);
			}

			ObjectDataRecord(object[] fields, int fieldCount) {
				this.values = fields;
				this.fieldCount = fieldCount;
			}

			public int FieldCount => fieldCount;

			public object this[int i] => GetValue(i);
			public object this[string name] => throw new NotSupportedException();

			public bool IsDBNull(int i) => DBNull.Value == values[i];

			public object GetValue(int i) => values[i];

			public bool GetBoolean(int i) => (bool)values[i];
			public DateTime GetDateTime(int i) => (DateTime)values[i];
			public Guid GetGuid(int i) => (Guid)values[i];

			public byte GetByte(int i) => (byte)values[i];
			public char GetChar(int i) => (char)values[i];

			public short GetInt16(int i) => (short)values[i];
			public int GetInt32(int i) => (int)values[i];
			public long GetInt64(int i) => (long)values[i];

			public float GetFloat(int i) => (float)values[i];
			public double GetDouble(int i) => (double)values[i];
			public decimal GetDecimal(int i) => (decimal)values[i];

			public string GetString(int i) => (string)values[i];

			public int GetValues(object[] values) {
				var n = Math.Min(fieldCount, values.Length);
				for (var i = 0; i != n; ++i)
					values[i] = GetValue(i);
				return n;
			}

			public long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length) {
				throw new NotImplementedException();
			}

			public long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length) {
				throw new NotImplementedException();
			}

			public IDataReader GetData(int i) {
				throw new NotImplementedException();
			}

			public string GetDataTypeName(int i) {
				throw new NotImplementedException();
			}

			public Type GetFieldType(int i) {
				throw new NotImplementedException();
			}

			public string GetName(int i) {
				throw new NotImplementedException();
			}

			public int GetOrdinal(string name) {
				throw new NotImplementedException();
			}
		}
	}
}
