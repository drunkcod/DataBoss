using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;

namespace DataBoss.Data
{
	public class ConcatDataReader : IDataReader
	{
		IDataReader current;
		Func<int, object> getProviderSpecificValue;
		readonly IDataReader[] readers;
		int next;
		readonly DataReaderSchemaTable schema;

		ConcatDataReader(DataReaderSchemaTable schema, IDataReader[] readers) {
			if (readers.Length == 0)
				throw new InvalidOperationException();
			this.readers = readers;
			this.current = readers[0];
			this.next = 1;
			this.schema = schema;
		}

		public static ConcatDataReader Create(IDataReader[] readers) {
			if (readers.Length == 0)
				throw new InvalidOperationException();

			var resultSchema = readers[0].GetDataReaderSchemaTable();
			for(var i = 1; i != readers.Length; ++i) {
				var readerSchema = readers[i].GetDataReaderSchemaTable();
				if (readerSchema.Count < resultSchema.Count)
					throw new InvalidOperationException($"Too few Columns available for reader {i}.");
				for(var o = 0; o != resultSchema.Count; ++o) {
					var expected = resultSchema[o];
					var actual = readerSchema[o];

					if (actual.ColumnName != expected.ColumnName)
						ColumnMismatch(o, nameof(DataReaderSchemaRow.ColumnName), expected.ColumnName, actual.ColumnName);
					
					if (actual.ColumnType != expected.ColumnType)
						ColumnMismatch(o, nameof(DataReaderSchemaRow.ColumnType), expected.ColumnType, actual.ColumnType);

					if(actual.AllowDBNull && !expected.AllowDBNull)
						ColumnMismatch(o, nameof(DataReaderSchemaRow.AllowDBNull), expected.AllowDBNull, actual.AllowDBNull);

					if (actual.ProviderSpecificDataType != expected.ProviderSpecificDataType)
						resultSchema[o].ProviderSpecificDataType = null;
				}
			}

			if(readers.Any(x => x is ConcatDataReader)) {
				var allReaders = new List<IDataReader>();
				foreach (var item in readers)
					if (item is ConcatDataReader cat)
						allReaders.AddRange(cat.readers);
					else allReaders.Add(item);
				readers = allReaders.ToArray();
			}

			return new ConcatDataReader(resultSchema, readers);
		}

		static void ColumnMismatch(int ordinal, string attributeName, object expected, object actual) =>
			throw new InvalidOperationException($"{attributeName} mismatch at column {ordinal}. Expected {expected} but got {actual}.");

		public int ReaderCount => readers.Length;

		public object this[int i] => current[i];
		public object this[string name] => current[name];

		public int Depth => throw new NotImplementedException();

		public bool IsClosed => current.IsClosed;
		public int RecordsAffected => 0;
		public int FieldCount => current.FieldCount;

		public void Close() {
			do {
				current.Close();
			} while (NextReader());
		}

		public void Dispose() {
			while (NextReader())
				;
			current.Dispose();
		}

		bool NextReader() {
			if(next != readers.Length) {
				current.Dispose();
				current = readers[next++];
				getProviderSpecificValue = null;
				return true;
			}
			return false;
		}

		public bool GetBoolean(int i) => current.GetBoolean(i);
		public byte GetByte(int i) => current.GetByte(i);
		public long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length) => current.GetBytes(i, fieldOffset, buffer, bufferoffset, length);
		public char GetChar(int i) => current.GetChar(i);
		public long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length) => current.GetChars(i, fieldoffset, buffer, bufferoffset, length);
		public IDataReader GetData(int i) => current.GetData(i);
		public DateTime GetDateTime(int i) => current.GetDateTime(i);
		public decimal GetDecimal(int i) => current.GetDecimal(i);
		public double GetDouble(int i) => current.GetDouble(i);
		public float GetFloat(int i) => current.GetFloat(i);
		public Guid GetGuid(int i) => current.GetGuid(i);
		public short GetInt16(int i) => current.GetInt16(i);
		public int GetInt32(int i) => current.GetInt32(i);
		public long GetInt64(int i) => current.GetInt64(i);
		public string GetString(int i) => current.GetString(i);
		public object GetValue(int i) => current.GetValue(i);
		public int GetValues(object[] values) => current.GetValues(values);
		public bool IsDBNull(int i) => current.IsDBNull(i);

		public string GetDataTypeName(int i) => schema[i].DataTypeName;
		public Type GetFieldType(int i) => schema[i].ColumnType;
		public string GetName(int i) => schema[i].ColumnName;
		public int GetOrdinal(string name) => schema.GetOrdinal(name);
		public DataTable GetSchemaTable() => schema.ToDataTable();
		public Type GetProviderSpecificFieldType(int i) => schema[i].ProviderSpecificDataType;

		public object GetProviderSpecificValue(int i) =>
			(getProviderSpecificValue ??= CreateGetProviderSpecificValueDelegate())(i);

		Func<int, object> CreateGetProviderSpecificValueDelegate() {
			var m = current.GetType().GetMethod(nameof(GetProviderSpecificValue), new[] { typeof(int) });
			return Lambdas.CreateDelegate<Func<int, object>>(current, m ?? throw new MissingMethodException($"Missing method {nameof(GetProviderSpecificValue)} on {current.GetType()}."));
		}

		public bool NextResult() => false;

		public bool Read() {
			while (current.Read() == false)
				if (!NextReader())
					return false;
			return true;
		}
	}
}
