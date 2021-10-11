using System;
using System.Collections;
using System.Data;
using System.Data.Common;
using System.Globalization;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using CsvHelper;
using DataBoss.Data;

namespace DataBoss.DataPackage
{
	public class CsvDataReader : DbDataReader, IDataRecordReader
	{		
		class CsvDataRecord : IDataRecord
		{
			static readonly Func<int, bool> NoData = InvalidGetAttempt;
			static bool InvalidGetAttempt(int i) => throw new InvalidOperationException("Invalid attempt to read when no data is present, call Read()");

			readonly string[] fieldValue;
			readonly BitArray isNull;
			readonly CsvDataReader parent;
			Func<int, bool> checkedIsNull;
			int rowNumber;

			public CsvDataRecord(CsvDataReader parent, BitArray isNull, string[] fieldValue) : this(parent, -1, isNull, fieldValue) 
			{ }
			
			CsvDataRecord(CsvDataReader parent, int rowNumber, BitArray isNull, string[] fieldValue) {
				this.isNull = isNull;
				this.fieldValue = fieldValue;
				this.parent = parent;
				this.rowNumber = rowNumber;
				this.checkedIsNull = rowNumber == -1 ? NoData : CheckedIsNullUnsafe;
			}

			public CsvDataRecord Clone() => new(parent, rowNumber, new BitArray(isNull), (string[])fieldValue.Clone());

			public void Fill(int rowNumber, CsvReader csv) {
				this.rowNumber = rowNumber;
				if(checkedIsNull == NoData)
					checkedIsNull = CheckedIsNullUnsafe;
				for (var i = 0; i != FieldCount; ++i) {
					var value = fieldValue[i] = csv.GetField(i);
					isNull[i] = IsNull(value);
				}
			}

			public object this[int i] => GetValue(i);
			public object this[string name] => GetValue(GetOrdinal(name));

			public int FieldCount => fieldValue.Length;

			public object GetValue(int i) {
				if (checkedIsNull(i))
					return DBNull.Value;

				try {
					return ChangeType(fieldValue[i], GetFieldType(i),  GetFieldFormat(i));
				} catch (FormatException ex) {
					var given = isNull[i] ? "null" : $"'{fieldValue[i]}'";
					throw new InvalidOperationException($"Failed to parse {GetName(i)} of type {GetFieldType(i)} given {given} on line {rowNumber}", ex);
				}
			}

			bool CheckedIsNullUnsafe(int i) {
				if (!isNull[i])
					return false;
				return parent.IsNullable(i) || UnexpectedNull(i);
			}

			bool UnexpectedNull(int i) => 
				throw new InvalidOperationException($"Unexpected null value for {GetName(i)} on line {rowNumber}.");

			public bool IsDBNull(int i) => isNull[i];

			public bool GetBoolean(int i) => GetFieldValue<bool>(i);
			public byte GetByte(int i) => GetFieldValue<byte>(i);
			public char GetChar(int i) => GetFieldValue<char>(i);
			public Guid GetGuid(int i) => GetFieldValue<Guid>(i);
			public short GetInt16(int i) => GetFieldValue<short>(i);
			public int GetInt32(int i) => GetFieldValue<int>(i);
			public long GetInt64(int i) => GetFieldValue<long>(i);
			public float GetFloat(int i) => GetFieldValue<float>(i);
			public double GetDouble(int i) => GetFieldValue<double>(i);
			public decimal GetDecimal(int i) => GetFieldValue<decimal>(i);
			public string GetString(int i) => checkedIsNull(i) ? default : fieldValue[i];
			public DateTime GetDateTime(int i) => (DateTime)GetValue(i);
			public TimeSpan GetTimeSpan(int i) => GetFieldValue<TimeSpan>(i);

			public CsvInteger GetCsvInteger(int i) => GetFieldValue<CsvInteger>(i);
			public CsvNumber GetCsvNumber(int i) => GetFieldValue<CsvNumber>(i);

			public T GetFieldValue<T>(int i) {
				if (checkedIsNull(i))
					return default;

				var value = fieldValue[i];

				if (typeof(T) == typeof(bool))
					return (T)(object)bool.Parse(value);
				if (typeof(T) == typeof(byte))
					return (T)(object)byte.Parse(value, GetFieldFormat(i));
				if (typeof(T) == typeof(char))
					return (T)(object)char.Parse(value);
				if (typeof(T) == typeof(short))
					return (T)(object)short.Parse(value, GetFieldFormat(i));
				if (typeof(T) == typeof(int))
					return (T)(object)int.Parse(value, GetFieldFormat(i));
				if (typeof(T) == typeof(long))
					return (T)(object)long.Parse(value, GetFieldFormat(i));
				if (typeof(T) == typeof(float))
					return (T)(object)float.Parse(value, GetFieldFormat(i));
				if (typeof(T) == typeof(double))
					return (T)(object)double.Parse(value, GetFieldFormat(i));
				if (typeof(T) == typeof(decimal))
					return (T)(object)decimal.Parse(value, GetFieldFormat(i));

				if (typeof(T) == typeof(TimeSpan))
					return (T)(object)TimeSpan.Parse(value, GetFieldFormat(i));

				if (typeof(T) == typeof(Guid))
					return (T)(object)Guid.Parse(value);

				if (typeof(T) == typeof(CsvInteger))
					return (T)(object)new CsvInteger(value, GetFieldFormat(i));
				if (typeof(T) == typeof(CsvNumber))
					return (T)(object)new CsvNumber(value, GetFieldFormat(i));

				return (T)GetValue(i);
			}

			public int GetValues(object[] values) {
				var n = Math.Min(FieldCount, values.Length);
				for (var i = 0; i != n; ++i)
					values[i] = GetValue(i);
				return n;
			}

			public IDataReader GetData(int i) => throw new NotSupportedException();
			public long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferOffset, int length) => this.GetArray(i, fieldOffset, buffer, bufferOffset, length);
			public long GetChars(int i, long fieldOffset, char[] buffer, int bufferOffset, int length) => this.GetArray(i, fieldOffset, buffer, bufferOffset, length);

			public string GetDataTypeName(int i) => parent.GetDataTypeName(i);
			public Type GetFieldType(int i) => parent.GetFieldType(i);
			IFormatProvider GetFieldFormat(int i) => parent.fieldFormat[i];

			public string GetName(int i) => parent.GetName(i);
			public int GetOrdinal(string name) => parent.GetOrdinal(name);
		}

		class FieldToDbTypeConverter
		{
			readonly Type Nullable = typeof(Nullable<>);
			readonly Func<string, bool> IsPrimaryKey;

			public FieldToDbTypeConverter(Func<string, bool> isPrimaryKey) {
				this.IsPrimaryKey = isPrimaryKey;
			}

			public (TableSchemaType TableType, DataBossDbType DbType) ToDbType(TabularDataSchemaFieldDescription field) {
				var isRequired = field.Constraints?.IsRequired ?? IsPrimaryKey(field.Name);
				var tableType = TableSchemaType.From(field.Type, field.Format);
				return (tableType, GetDbType(isRequired, tableType.Type));
			}

			DataBossDbType GetDbType(bool isRequired, Type type) {
				if (isRequired)
					return DataBossDbType.From(type, RequiredAttributeProvider.Instance);

				if (type.IsValueType)
					return DataBossDbType.From(Nullable.MakeGenericType(type));

				return DataBossDbType.From(type);
			}
		}

		readonly CsvDataRecord current;
		readonly IFormatProvider[] fieldFormat;
		readonly CsvTypeCode[] csvFieldType;
		readonly DataReaderSchemaTable schema;
		CsvReader csv;
		int rowNumber;

		public event EventHandler Disposed;

		public CsvDataReader(TextReader csv, CultureInfo cultureInfo, TabularDataSchema tabularSchema, bool hasHeaderRow = true) :
			this(new CsvReader(csv, cultureInfo), tabularSchema, hasHeaderRow) 
		{ }

		public CsvDataReader(CsvReader csv, TabularDataSchema tabularSchema, bool hasHeaderRow = true) {
			var fieldCount = tabularSchema.Fields.Count;

			this.csv = csv;
			this.schema = new DataReaderSchemaTable();
			this.fieldFormat = new IFormatProvider[fieldCount];
			this.csvFieldType = new CsvTypeCode[fieldCount];
			this.current = new CsvDataRecord(this, new BitArray(fieldCount), new string[fieldCount]);

			TranslateSchema(tabularSchema);

			if (hasHeaderRow)
				ValidateHeaderRow();
		}

		void TranslateSchema(TabularDataSchema tabularSchema) {
			var fieldTypeConverter = new FieldToDbTypeConverter(tabularSchema.PrimaryKey == null ? _ => false : tabularSchema.PrimaryKey.Contains);
			for (var i = 0; i != tabularSchema.Fields.Count; ++i) {
				var field = tabularSchema.Fields[i];
				var (tableType, dbType) = fieldTypeConverter.ToDbType(field);
				csvFieldType[i] = tableType.CsvTypeCode;
				schema.Add(
					field.Name, 
					i, 
					tableType.Type, 
					dbType.IsNullable, 
					field.Constraints?.MaxLength ?? dbType.ColumnSize, 
					dbType.TypeName,
					tableType.CsvType);
				
				if (field.IsNumber())
					fieldFormat[i] = field.GetNumberFormat();
			}
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

		bool ReadRow() => UpdateRowNumber(csv.Read());
		async Task<bool> ReadRowAsync() => UpdateRowNumber(await csv.ReadAsync());

		bool UpdateRowNumber(bool ok) {
			if (!ok)
				return false;
			++rowNumber;
			return true;
		}

		public override int FieldCount => schema.Count;
		public override bool IsClosed => csv == null;
		public override int Depth => throw new NotSupportedException();
		public override int RecordsAffected => throw new NotSupportedException();
		public override bool HasRows => throw new NotSupportedException();

		public override DataTable GetSchemaTable() => schema.ToDataTable();
		
		public override bool Read() => FillOnRead(ReadRow());
		public override async Task<bool> ReadAsync(CancellationToken cancellationToken) => FillOnRead(await ReadRowAsync());

		bool FillOnRead(bool rowRead) {
			if (!rowRead)
				return false;
			current.Fill(rowNumber, csv);
			return true;
		}

		public override bool NextResult() => false;

		static bool IsNull(string input) => string.IsNullOrEmpty(input);

		bool IsNullable(int i) => schema[i].AllowDBNull;

		static object ChangeType(string input, Type type, IFormatProvider format) =>
			Type.GetTypeCode(type) switch {
				TypeCode.DateTime => DateTime.Parse(input, format),
				TypeCode.Object when type == typeof(TimeSpan) => TimeSpan.Parse(input, format),
				TypeCode.Object when type == typeof(byte[]) => Convert.FromBase64String(input),
				_ => Convert.ChangeType(input, type, format),
			};

		public override Type GetFieldType(int i) => schema[i].DataType;
		public override Type GetProviderSpecificFieldType(int i) => schema[i].ProviderSpecificDataType;
		public override string GetDataTypeName(int i) => schema[i].DataTypeName;
		public override string GetName(int i) => schema[i].ColumnName;
		public override int GetOrdinal(string name) => schema.GetOrdinal(name);

		public override object this[int i] => GetValue(i);
		public override object this[string name] => GetValue(GetOrdinal(name));

		public override void Close() {
			csv.Dispose();
			csv = null;
		}

		protected override void Dispose(bool disposing) {
			if(!disposing)
				return;

			csv?.Dispose();
			Disposed?.Invoke(this, EventArgs.Empty);
		}

		public override object GetValue(int i) => current.GetValue(i);
		public override bool IsDBNull(int i) => current.IsDBNull(i);

		public override bool GetBoolean(int i) => current.GetBoolean(i);
		public override byte GetByte(int i) => current.GetByte(i);
		public override char GetChar(int i) => current.GetChar(i);
		public override Guid GetGuid(int i) => current.GetGuid(i);
		public override short GetInt16(int i) => current.GetInt16(i);
		public override int GetInt32(int i) => current.GetInt32(i);
		public override long GetInt64(int i) => current.GetInt64(i);
		public override float GetFloat(int i) => current.GetFloat(i);
		public override double GetDouble(int i) => current.GetDouble(i);
		public override string GetString(int i) => current.GetString(i);
		public override decimal GetDecimal(int i) => current.GetDecimal(i);
		public override DateTime GetDateTime(int i) => current.GetDateTime(i);
		public TimeSpan GetTimeSpan(int i) => current.GetTimeSpan(i);

		public CsvInteger GetCsvInteger(int i) => current.GetCsvInteger(i);
		public CsvNumber GetCsvNumber(int i) => current.GetCsvNumber(i);

		public override T GetFieldValue<T>(int ordinal) => current.GetFieldValue<T>(ordinal);

		public override object GetProviderSpecificValue(int i) =>
			csvFieldType[i] switch {
				CsvTypeCode.None => throw new InvalidCastException(),
				CsvTypeCode.CsvInteger => GetCsvInteger(i),
				CsvTypeCode.CsvNumber => GetCsvNumber(i),
				_ => throw new InvalidOperationException(),
			};

		public override int GetValues(object[] values) {
			var n = Math.Min(FieldCount, values.Length);
			for(var i = 0; i != n; ++i)
				values[i] = GetValue(i);
			return n;
		}

		public override long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferOffset, int length) => this.GetArray(i, fieldOffset, buffer, bufferOffset, length);
		public override long GetChars(int i, long fieldOffset, char[] buffer, int bufferOffset, int length) => this.GetArray(i, fieldOffset, buffer, bufferOffset, length);

		public IDataRecord GetRecord() => current.Clone();

		public override IEnumerator GetEnumerator() => new DataReaderEnumerator(this);

	}
}
