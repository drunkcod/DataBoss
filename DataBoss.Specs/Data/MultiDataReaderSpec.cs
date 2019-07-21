using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Cone;
using DataBoss.Data;
using DataBoss.Data.Common;

namespace DataBoss.Data
{
	class MultiDataReader : IDataReader
	{ 
		readonly IDataReader[] readers;
		int next;
		IDataReader currentReader;
		public MultiDataReader(params IDataReader[] readers) { 
			this.readers = readers;
			this.currentReader = readers[0];
		}


		public object this[int i] => currentReader[i];
		public object this[string name] => currentReader[name];
		public int FieldCount => currentReader.FieldCount;

		public int Depth => throw new NotImplementedException();

		public bool IsClosed => throw new NotImplementedException();

		public int RecordsAffected => throw new NotImplementedException();

		public void Close() {
			throw new NotImplementedException();
		}

		public void Dispose() {
			throw new NotImplementedException();
		}

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

			if(readers[next].Read()) {
				currentReader = readers[next];
				next = (next + 1) % readers.Length;
				return true;
			}
			return false;
		}
	}

	[Describe(typeof(MultiDataReader))]
	public class MultiDataReaderSpec
	{
		public void reads_elements_in_round_robin_fashion() 
		{
			var reader = new MultiDataReader(
				SequenceDataReader.Create(new[]{ 1, 3, 5 }.Select(x => new IdRow<int> { Id = x })),
				SequenceDataReader.Create(new[]{ 2, 4 }.Select(x => new IdRow<int> { Id = x })));

			Check.With(() => ObjectReader.For(reader).Read<IdRow<int>>().ToList()).That(
				xs => xs.Count == 5,
				xs => xs[0].Id == 1,
				xs => xs[1].Id == 2);
		}
	}
}
 