using System;
using System.Data;
using CsvHelper;
using DataBoss.Data;

namespace DataBoss.DataPackage
{
	class CsvDataReader : IDataReader
	{
		readonly CsvReader csv;
		readonly DataReaderSchemaTable schema;
		readonly DataBossDbType[] dbTypes;

		public CsvDataReader(CsvReader csv, DataPackageTabularSchema tabularSchema, bool hasHeaders = true) { 
			this.csv = csv; 
			this.schema = new DataReaderSchemaTable();
			this.dbTypes = new DataBossDbType[tabularSchema.Fields.Count];
			for (var i = 0; i != tabularSchema.Fields.Count; ++i) {
				var (fieldType, dbType) = ToDbType(tabularSchema.Fields[i]);
				schema.Add(tabularSchema.Fields[i].Name, i, fieldType, dbType.IsNullable, dbType.ColumnSize);
				dbTypes[i] = dbType;
			}

			if (hasHeaders) {
				csv.Read();
				for(var i = 0; i != tabularSchema.Fields.Count; ++i) {
					var expected = tabularSchema.Fields[i].Name;
					var actual = csv.GetField(i);
					if(actual != expected) 
						throw new InvalidOperationException($"Header mismatch, expected '{expected}' got {actual}");
				}
			}
		}

		static (Type, DataBossDbType) ToDbType(DataPackageTabularFieldDescription field) {
			switch(field.Type) {
				default: throw new NotSupportedException($"Don't know how to map '{field.Type}'");
				case "integer": return (typeof(int), DataBossDbType.ToDataBossDbType(typeof(int)));
				case "string": return (typeof(string), DataBossDbType.ToDataBossDbType(typeof(int)));
			}
		}

		public int FieldCount => schema.Count;

		public DataTable GetSchemaTable() => schema.ToDataTable();
		public bool Read() => csv.Read();
		public bool NextResult() => false;

		public object GetValue(int i) => Convert.ChangeType(csv.GetField(i), GetFieldType(i));
		public Type GetFieldType(int i) => schema[i].FieldType;
		public string GetDataTypeName(int i) => dbTypes[i].TypeName;
		public string GetName(int i) => schema[i].ColumnName;
		public int GetOrdinal(string name) => schema.GetOrdinal(name);

		public object this[int i] => GetValue(i);
		public object this[string name] => GetValue(GetOrdinal(name));

		public void Close() => csv.Context.Reader.Close();
		public void Dispose() => csv.Dispose();

		public bool IsDBNull(int i) => GetValue(i) is DBNull;

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
		public int GetValues(object[] values) {
			var n = Math.Min(FieldCount, values.Length);
			for(var i = 0; i != n; ++i)
				values[i] = GetValue(i);
			return n;
		}

		public IDataReader GetData(int i) => throw new NotSupportedException();
	}
}
