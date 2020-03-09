using System;
using System.Data;
using CsvHelper;
using DataBoss.Data;

namespace DataBoss.DataPackage
{
	public class CsvDataReader : IDataReader
	{
		readonly CsvReader csv;
		readonly DataReaderSchemaTable schema;
		readonly string[] primaryKey;
		readonly DataBossDbType[] dbTypes;
		readonly IFormatProvider[] fieldFormat;

		readonly bool[] isNull;
		int rowNumber;
	
		public event EventHandler Disposed;

		public CsvDataReader(CsvReader csv, TabularDataSchema tabularSchema, bool hasHeaderRow = true) { 
			this.csv = csv; 
			this.schema = new DataReaderSchemaTable();
			this.primaryKey = tabularSchema.PrimaryKey?.ToArray() ?? Empty<string>.Array;
			this.dbTypes = new DataBossDbType[tabularSchema.Fields.Count];
			this.fieldFormat = new IFormatProvider[tabularSchema.Fields.Count];
			for (var i = 0; i != tabularSchema.Fields.Count; ++i) {
				var field = tabularSchema.Fields[i];
				var (fieldType, dbType) = ToDbType(field);
				schema.Add(field.Name, i, fieldType, dbType.IsNullable, dbType.ColumnSize);
				dbTypes[i] = dbType;
				if(field.IsNumber())
					fieldFormat[i] = field.GetNumberFormat();
			}
			this.isNull = new bool[FieldCount];

			if (hasHeaderRow)
				ValidateHeaderRow();
		}

		void ValidateHeaderRow() {
			if(!ReadRow())
				throw new InvalidOperationException("Missing header row.");
			for (var i = 0; i != schema.Count; ++i) {
				var expected = schema[i].ColumnName;
				var actual = csv.GetField(i);
				if (actual != expected)
					throw new InvalidOperationException($"Header mismatch, expected '{expected}' got {actual}");
			}
		}

		bool ReadRow() {
			var ok = csv.Read();
			if(!ok)
				return false;
			++rowNumber;
			return true;
		}

		(Type, DataBossDbType) ToDbType(TabularDataSchemaFieldDescription field) {
			var required = field.Constraints?.IsRequired ?? Array.IndexOf(primaryKey, field.Name) != -1;
			switch(field.Type) {
				default: throw new NotSupportedException($"Don't know how to map '{field.Type}'");
				case "boolean": return GetDbTypePair(typeof(bool), required);
				case "datetime": return GetDbTypePair(typeof(DateTime), required);
				case "date": return GetDbTypePair(typeof(DateTime), required);
				case "time": return GetDbTypePair(typeof(TimeSpan), required);
				case "integer": return GetDbTypePair(typeof(int), required);
				case "number": return GetDbTypePair(typeof(double), required);
				case "string": return GetDbTypePair(typeof(string), required);
			}
		}

		static (Type, DataBossDbType) GetDbTypePair(Type type, bool required = false) =>
			(type, DataBossDbType.ToDataBossDbType(!type.IsValueType || required ? type : typeof(Nullable<>).MakeGenericType(type)));

		public int FieldCount => schema.Count;

		public DataTable GetSchemaTable() => schema.ToDataTable();
		
		public bool Read() {
			var ok = ReadRow();
			if(!ok)
				return false;
			for (var i = 0; i != FieldCount; ++i)
				isNull[i] = IsNull(csv.GetField(i));
			return true;
		}

		static bool IsNull(string input) => string.IsNullOrEmpty(input);

		public bool NextResult() => false;

		public object GetValue(int i) {
			if (CheckedIsNull(i))
				return DBNull.Value;
			
			try {
				return ChangeType(csv.GetField(i), GetFieldType(i), fieldFormat[i]);
			}
			catch (FormatException ex) {
				var given = isNull[i] ? "null" : $"'{csv.GetField(i)}'";
				throw new InvalidOperationException($"Failed to parse {GetName(i)} of type {GetFieldType(i)} given {given} on line {rowNumber}", ex);
			}
		}

		object ChangeType(string input, Type type, IFormatProvider format) {

			switch(Type.GetTypeCode(type)) {
				case TypeCode.DateTime:
					var value = DateTime.Parse(input, format);
					if(value.Kind == DateTimeKind.Unspecified);
						value = DateTime.SpecifyKind(value, DateTimeKind.Utc);
					return value;
				case TypeCode.Object:
					if (type == typeof(TimeSpan))
						return TimeSpan.Parse(input, format);
					break;
			}
			
			return Convert.ChangeType(input, type, format);
		}

		bool CheckedIsNull(int i) {
			if (isNull[i])
				return dbTypes[i].IsNullable ? true : throw new InvalidOperationException($"Unexpected null value for {GetName(i)} on line {rowNumber}.");
			return false;
		}

		public Type GetFieldType(int i) => schema[i].ColumnType;
		public string GetDataTypeName(int i) => dbTypes[i].TypeName;
		public string GetName(int i) => schema[i].ColumnName;
		public int GetOrdinal(string name) => schema.GetOrdinal(name);

		public object this[int i] => GetValue(i);
		public object this[string name] => GetValue(GetOrdinal(name));

		public void Close() => csv.Context.Reader.Close();
		public void Dispose() {
			csv.Dispose();
			Disposed?.Invoke(this, EventArgs.Empty);
		}

		public bool IsDBNull(int i) => isNull[i];

		int IDataReader.Depth => throw new NotSupportedException();
		bool IDataReader.IsClosed => throw new NotSupportedException();
		int IDataReader.RecordsAffected => throw new NotSupportedException();

		public bool GetBoolean(int i) => (bool)GetValue(i);
		public byte GetByte(int i) => (byte)GetValue(i);
		public char GetChar(int i) => (char)GetValue(i);
		public Guid GetGuid(int i) => (Guid)GetValue(i);
		public short GetInt16(int i) => CheckedIsNull(i) ? default : short.Parse(csv.GetField(i), fieldFormat[i]);
		public int GetInt32(int i) => CheckedIsNull(i) ? default : int.Parse(csv.GetField(i), fieldFormat[i]);
		public long GetInt64(int i) => CheckedIsNull(i) ? default : long.Parse(csv.GetField(i), fieldFormat[i]);
		public float GetFloat(int i) => CheckedIsNull(i) ? default : float.Parse(csv.GetField(i), fieldFormat[i]);
		public double GetDouble(int i) => CheckedIsNull(i) ? default : double.Parse(csv.GetField(i), fieldFormat[i]);
		public string GetString(int i) => CheckedIsNull(i) ? default: csv.GetField(i);
		public decimal GetDecimal(int i) => CheckedIsNull(i) ? default : decimal.Parse(csv.GetField(i), fieldFormat[i]);
		public DateTime GetDateTime(int i) => (DateTime)GetValue(i);
		public TimeSpan GetTimeSpan(int i) => CheckedIsNull(i) ? default : TimeSpan.Parse(csv.GetField(i), fieldFormat[i]);

		public int GetValues(object[] values) {
			var n = Math.Min(FieldCount, values.Length);
			for(var i = 0; i != n; ++i)
				values[i] = GetValue(i);
			return n;
		}

		public IDataReader GetData(int i) => throw new NotSupportedException();
		public long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length) => throw new NotImplementedException();
		public long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length) => throw new NotImplementedException();
	}
}
