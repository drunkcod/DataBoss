using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using DataBoss.Data;

namespace DataBoss.Specs
{
	class SimpleDataReader : IDataReader, IEnumerable<object[]>
	{
		class SimpleDataReaderResult
		{
			public KeyValuePair<string, Type>[] Fields;
			public List<object[]> Records = new List<object[]>();
			public DataTable schema = new DataTable();
			public int currentRecord;

		}

		Queue<SimpleDataReaderResult> results;
		SimpleDataReaderResult current;
		SimpleDataReaderResult addTo;
		DataTable Schema => current.schema;
		KeyValuePair<string, Type>[] Fields => current.Fields;
		List<object[]> Records => current.Records;

		public event EventHandler<EventArgs> Closed;

		public SimpleDataReader(params KeyValuePair<string, Type>[] fields) {
			this.current = this.addTo = NewResult(fields);
		}

		public void AddResult(params KeyValuePair<string, Type>[] fields) =>
			(results ??= new()).Enqueue(addTo = NewResult(fields));

		static SimpleDataReaderResult NewResult(params KeyValuePair<string, Type>[] fields) {
			var result = new SimpleDataReaderResult {
				Fields = fields,

			};
			var ordinal = result.schema.Columns.Add(DataReaderSchemaColumns.ColumnOrdinal);
			var isNullable = result.schema.Columns.Add(DataReaderSchemaColumns.AllowDBNull);
			for (var i = 0; i != fields.Length; ++i) {
				var row = result.schema.NewRow();
				row[ordinal] = i;
				row[isNullable] = false;
				result.schema.Rows.Add(row);
			}
			return result;
		}

		public void Add(params object[] record) {

			if(record.Length != addTo.Fields.Length)
				throw new InvalidOperationException("Invalid record length");
			addTo.Records.Add(record);
		}

		public void SetNullable(int ordinal, bool isNullable) 
		{
			Schema.Rows[ordinal][DataReaderSchemaColumns.AllowDBNull.Name] = isNullable;
		}

		public int Count => Records.Count;
		public int FieldCount => Fields.Length;

		public bool Read() {
			if(current.currentRecord == Records.Count)
				return false;
			++current.currentRecord;
			return true;
		}
		public string GetName(int i) => Fields[i].Key;
		public Type GetFieldType(int i) => Fields[i].Value;
		public object GetValue(int i) => Records[current.currentRecord - 1][i];

		public void Close() {
			Closed?.Invoke(this, EventArgs.Empty);  
		}
		public void Dispose() => Close();

		public string GetDataTypeName(int i) { throw new NotImplementedException(); }
		public int GetValues(object[] values) { throw new NotImplementedException(); }
		public int GetOrdinal(string name) => Array.FindIndex(Fields, x => x.Key == name);
	
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

		object IDataRecord.this[int i] => Records[i];
		object IDataRecord.this[string name] => GetValue(GetOrdinal(name));

		public DataTable GetSchemaTable() => Schema;

		public bool NextResult() {
			if ((results?.Count ?? 0) == 0)
				return false;
			current = results.Dequeue();
			return true;
		}

		public bool IsClosed => false;

		public int Depth { get { throw new NotImplementedException(); } }
		public int RecordsAffected { get { throw new NotImplementedException(); } }

		IEnumerator<object[]> IEnumerable<object[]>.GetEnumerator() => Records.GetEnumerator();
		IEnumerator IEnumerable.GetEnumerator() => ((IEnumerable) Records).GetEnumerator();
	}
}