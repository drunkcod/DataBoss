using System;
using System.Collections.Generic;
using System.Data;
using System.Linq.Expressions;
using System.Reflection;

namespace DataBoss.Data
{
	public static class SequenceDataReader
	{
		public static IDataReader Items<T>(params T[] data) => Create(data);
		public static IDataReader Create<T>(IEnumerable<T> data) => Create(data, x => x.MapAll());
		public static IDataReader Create<T>(IEnumerable<T> data, Action<FieldMapping<T>> mapFields) {
			var fieldMapping = new FieldMapping<T>();
			mapFields(fieldMapping);
			return new SequenceDataReader<T>(data?.GetEnumerator(), fieldMapping);
		}

		public static IDataReader Create<T>(IEnumerable<T> data, params string[] members) =>
			Create(data, fields => Array.ForEach(members, x => fields.Map(x)));

		public static IDataReader Create<T>(IEnumerable<T> data, params MemberInfo[] members) =>
			Create(data, fields => Array.ForEach(members, x => fields.Map(x)));

		public static IDataReader ToDataReader<T>(this IEnumerable<T> data) => Create(data); 
	}

	public class SequenceDataReader<T> : IDataReader, IDataRecordReader
	{
		interface IColumnAccessor
		{
			bool IsDBNull(T item);
			object GetValue(T item);
			TValue GetFieldValue<TValue>(T item);
		}

		sealed class ColumnAccessor<TField> : IColumnAccessor
		{
			readonly Func<T, TField> get;
			readonly Func<T, bool> hasValue;

			public ColumnAccessor(Func<T, TField> get, Func<T, bool> hasValue) {
				this.get = get;
				this.hasValue = hasValue;
			}

			public object GetValue(T item) =>
				IsDBNull(item) ? DBNull.Value : (object)get(item);

			public bool IsDBNull(T item) =>
				(hasValue != null) && !hasValue(item);

			public bool GetBoolean(T item) => GetFieldValue<bool>(item);
			public byte GetByte(T item) => GetFieldValue<byte>(item);
			public char GetChar(T item) => GetFieldValue<char>(item);
			public short GetInt16(T item) => GetFieldValue<short>(item);
			public int GetInt32(T item) => GetFieldValue<int>(item);
			public long GetInt64(T item) => GetFieldValue<long>(item);
			public float GetFloat(T item) => GetFieldValue<float>(item);
			public double GetDouble(T item) => GetFieldValue<double>(item);
			public decimal GetDecimal(T item) => GetFieldValue<decimal>(item);
			public DateTime GetDateTime(T item) => GetFieldValue<DateTime>(item);
			public Guid GetGuid(T item) => GetFieldValue<Guid>(item);
			public string GetString(T item) => GetFieldValue<string>(item);

			public TValue GetFieldValue<TValue>(T item) =>
				(TValue)(object)get(item);
		}

		class DataRecord : IDataRecord
		{
			readonly SequenceDataReader<T> parent;
			readonly T item;

			public DataRecord(SequenceDataReader<T> parent, T item) {
				this.parent = parent;
				this.item = item; 
			}

			public int FieldCount => parent.FieldCount;

			public bool IsDBNull(int i) => GetAccessor(i).IsDBNull(item);
			public object GetValue(int i) => GetAccessor(i).GetValue(item);

			public bool GetBoolean(int i) => GetAccessor(i).GetFieldValue<bool>(item);
			public byte GetByte(int i) => GetAccessor(i).GetFieldValue<byte>(item);
			public char GetChar(int i) => GetAccessor(i).GetFieldValue<char>(item);
			public short GetInt16(int i) => GetAccessor(i).GetFieldValue<short>(item);
			public int GetInt32(int i) => GetAccessor(i).GetFieldValue<int>(item);
			public long GetInt64(int i) => GetAccessor(i).GetFieldValue<long>(item);
			public float GetFloat(int i) => GetAccessor(i).GetFieldValue<float>(item);
			public double GetDouble(int i) => GetAccessor(i).GetFieldValue<double>(item);
			public decimal GetDecimal(int i) => GetAccessor(i).GetFieldValue<decimal>(item);
			public DateTime GetDateTime(int i) => GetAccessor(i).GetFieldValue<DateTime>(item);
			public Guid GetGuid(int i) => GetAccessor(i).GetFieldValue<Guid>(item);
			public string GetString(int i) => GetAccessor(i).GetFieldValue<string>(item);

			IColumnAccessor GetAccessor(int i) => parent.accessors[i];

			public int GetValues(object[] values) {
				var n = Math.Min(FieldCount, values.Length);
				for (var i = 0; i != n; ++i)
					values[i] = GetValue(i);
				return n;
			}

			public object this[int i] => GetValue(i);
			public object this[string name] => GetValue(GetOrdinal(name));

			public long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length) => throw new NotImplementedException();
			public long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length) => throw new NotImplementedException();
			public IDataReader GetData(int i) => throw new NotImplementedException();

			public string GetDataTypeName(int i) => parent.GetDataTypeName(i);
			public Type GetFieldType(int i) => parent.GetFieldType(i);
			public string GetName(int i) => parent.GetName(i);
			public int GetOrdinal(string name) => parent.GetOrdinal(name);
		}

		readonly IColumnAccessor[] accessors;
		readonly IEnumerator<T> data;
		readonly DataReaderSchemaTable schema = new DataReaderSchemaTable();
	
		internal SequenceDataReader(IEnumerator<T> data, FieldMapping<T> fields) {
			this.data = data ?? throw new ArgumentNullException(nameof(data));
			var fieldNames = fields.GetFieldNames();
			var fieldTypes = fields.GetFieldTypes();
			var dbTypes = fields.GetDbTypes();
			for(var i = 0; i != fieldNames.Length; ++i) 
				schema.Add(fieldNames[i], i, fieldTypes[i], dbTypes[i].IsNullable, columnSize: dbTypes[i].ColumnSize, dataTypeName: dbTypes[i].TypeName); 
			this.accessors = new IColumnAccessor[fields.Count];
			for (var i = 0; i != accessors.Length; ++i)
				accessors[i] = MakeAccessor(fields.Source, fields[i], schema[i].IsValueType);
		}

		static IColumnAccessor MakeAccessor(ParameterExpression source, in FieldMappingItem field, bool isValueType) {
			var (hasValue, selector) = field.HasValue == null ? (null, field.Selector) : (CompileSelector<bool>(source, field.HasValue), field.GetValue);
			switch(Type.GetTypeCode(selector.Type)) {
				default:
					if (selector.Type == typeof(Guid))
						return new ColumnAccessor<Guid>(CompileSelector<Guid>(source, selector), hasValue);
					return new ColumnAccessor<object>(CompileSelector<object>(source, selector.Box()), hasValue);
				case TypeCode.Boolean: return new ColumnAccessor<bool>(CompileSelector<bool>(source, selector), hasValue);
				case TypeCode.Byte: return new ColumnAccessor<byte>(CompileSelector<byte>(source, selector), hasValue);
				case TypeCode.Char: return new ColumnAccessor<char>(CompileSelector<char>(source, selector), hasValue);
				case TypeCode.Int16: return new ColumnAccessor<short>(CompileSelector<short>(source, selector), hasValue);
				case TypeCode.Int32: return new ColumnAccessor<int>(CompileSelector<int>(source, selector), hasValue);
				case TypeCode.Int64: return new ColumnAccessor<long>(CompileSelector<long>(source, selector), hasValue);
				case TypeCode.Single: return new ColumnAccessor<float>(CompileSelector<float>(source, selector), hasValue);
				case TypeCode.Double: return new ColumnAccessor<double>(CompileSelector<double>(source, selector), hasValue);
				case TypeCode.Decimal: return new ColumnAccessor<decimal>(CompileSelector<decimal>(source, selector), hasValue);
				case TypeCode.DateTime: return new ColumnAccessor<DateTime>(CompileSelector<DateTime>(source, selector), hasValue);
				case TypeCode.String: return new ColumnAccessor<string>(CompileSelector<string>(source, selector), hasValue);
			}			
		}

		static Func<T, TResult> CompileSelector<TResult>(ParameterExpression source, Expression selector) =>
			Expression.Lambda<Func<T, TResult>>(selector, source).Compile();

		public object this[int i] => GetValue(i);
		public object this[string name] => GetValue(GetOrdinal(name));

		public int FieldCount => schema.Count;
	
		public bool Read() { 
			if(!data.MoveNext())
				return false;
			return true;
		}

		public bool NextResult() => false;

		public void Close() { }

		public void Dispose() => data.Dispose();

		public string GetName(int i) => schema[i].ColumnName;
		public Type GetFieldType(int i) => schema[i].ColumnType;
		public string GetDataTypeName(int i) => schema[i].DataTypeName;
		public int GetOrdinal(string name) {
			var o = schema.GetOrdinal(name);
			if(o < 0)
				throw new InvalidOperationException($"No field named '{name}' mapped");
			return o;
		}

		public object GetValue(int i) => accessors[i].GetValue(data.Current);

		public int GetValues(object[] values) {
			var n = Math.Min(FieldCount, values.Length);
			for (var i = 0; i != n; ++i)
				values[i] = GetValue(i);
			return n;
		}

		DataTable IDataReader.GetSchemaTable() =>
			schema.ToDataTable();

		//SqlBulkCopy.EnableStreaming requires this
		public bool IsDBNull(int i) => IsDBNull(data.Current, i);

		bool IsDBNull(T item, int i) {
			var field = schema[i];
			if (!field.AllowDBNull)
				return false;
			var x = accessors[i].GetValue(item);
			return (field.IsValueType == false && x == null) || (DBNull.Value == x);
		}

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
		public IDataReader GetData(int i) => throw new NotImplementedException();

		public IDataRecord GetRecord() => new DataRecord(this, data.Current);
	}
}
