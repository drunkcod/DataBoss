using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq.Expressions;
using System.Reflection;

namespace DataBoss.Data
{
	public static class SequenceDataReader
	{
		public static DbDataReader Items<T>(params T[] data) => Create(data);
		public static DbDataReader Create<T>(IEnumerable<T> data) => Create(data, x => x.MapAll());
		public static DbDataReader Create<T>(IEnumerable<T> data, Action<FieldMapping<T>> mapFields) {
			var fieldMapping = new FieldMapping<T>();
			mapFields(fieldMapping);
			return new SequenceDataReader<T>(data?.GetEnumerator(), fieldMapping);
		}

		public static DbDataReader Create<T>(IEnumerable<T> data, params string[] members) =>
			Create(data, fields => Array.ForEach(members, x => fields.Map(x)));

		public static DbDataReader Create<T>(IEnumerable<T> data, params MemberInfo[] members) =>
			Create(data, fields => Array.ForEach(members, x => fields.Map(x)));

		public static DbDataReader ToDataReader<T>(this IEnumerable<T> data) => Create(data); 
	}

	public sealed class SequenceDataReader<T> : DbDataReader, IDataRecordReader
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
				IsDBNull(item) ? DBNull.Value : GetFieldValue<object>(item);

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

			public TValue GetFieldValue<TValue>(T item) {
				if(typeof(TValue) == typeof(TField) || typeof(TValue) == typeof(object))
					return (TValue)(object)get(item);
				ThrowInvalidCastException<TField, TValue>();
				return default;
			}
		}

		sealed class StringColumnAccessor : IColumnAccessor
		{
			readonly Func<T, string> get;

			public StringColumnAccessor(Func<T, string> get) { this.get = get; }

			public TValue GetFieldValue<TValue>(T item) {
				if (typeof(TValue) == typeof(string) || typeof(TValue) == typeof(object))
					return (TValue)(object)get(item);
				ThrowInvalidCastException<string, TValue>();
				return default;
			}

			public object GetValue(T item) => (object)get(item) ?? DBNull.Value;
			public bool IsDBNull(T item) => get(item) == null;
		}

		abstract class AccessorCollectionBase : IReadOnlyList<IColumnAccessor>
		{

			public abstract AccessorCollectionBase GetAccessors();

			public abstract int Count { get; }

			public abstract IColumnAccessor this[int index] { get; }

			public abstract IEnumerator<IColumnAccessor> GetEnumerator();
			IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
		}

		sealed class NoDataAccessorCollection : AccessorCollectionBase
		{
			readonly FieldMapping<T> mapping;

			static TValue NoData<TValue>() => throw new InvalidOperationException("Invalid attempt to read when no data is present, call Read()");

			public NoDataAccessorCollection(FieldMapping<T> mapping) {
				this.mapping = mapping;
			}

			public override int Count => mapping.Count;

			public override IColumnAccessor this[int index] => NoData<IColumnAccessor>();

			public override AccessorCollectionBase GetAccessors() {
				var accessors = new IColumnAccessor[mapping.Count];
				for (var i = 0; i != accessors.Length; ++i)
					accessors[i] = MakeAccessor(mapping.Source, mapping[i]);

				return new AccessorCollection(accessors);
			}

			public override IEnumerator<IColumnAccessor> GetEnumerator() => NoData<IEnumerator<IColumnAccessor>>();
		}

		sealed class AccessorCollection : AccessorCollectionBase
		{
			readonly IColumnAccessor[] columns;

			public AccessorCollection(IColumnAccessor[] columns) {
				this.columns = columns;
			}

			public override int Count => columns.Length;

			public override IColumnAccessor this[int index] => columns[index];

			public override IEnumerator<IColumnAccessor> GetEnumerator() => GetEnumerator(columns);

			static IEnumerator<TItem> GetEnumerator<TItem>(IEnumerable<TItem> xs) => xs.GetEnumerator();

			public override AccessorCollectionBase GetAccessors() => this;
		}

		static void ThrowInvalidCastException<TField, TValue>() => 
			throw new InvalidCastException($"Unable to cast object of type '{typeof(TField)}' to {typeof(TValue)}.");

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

		AccessorCollectionBase accessors;
		readonly IEnumerator<T> data;
		readonly DataReaderSchemaTable schema;

		internal SequenceDataReader(IEnumerator<T> data, FieldMapping<T> fields) {
			this.data = data ?? throw new ArgumentNullException(nameof(data));
			this.accessors = new NoDataAccessorCollection(fields);
			this.schema = GetSchema(fields);
		}


		static DataReaderSchemaTable GetSchema(FieldMapping<T> mapping) {
			var schema = new DataReaderSchemaTable();
			for (var i = 0; i != mapping.Count; ++i) {
				var dbType = mapping.GetDbType(i);
				schema.Add(
					ordinal: i,
					name: mapping.GetFieldName(i),
					columnType: mapping.GetFieldType(i),
					allowDBNull: dbType.IsNullable,
					columnSize: dbType.ColumnSize,
					dataTypeName: dbType.TypeName);
			}
			return schema;
		}

		static IColumnAccessor MakeAccessor(ParameterExpression source, in FieldMappingItem field) {
			if (field.FieldType == typeof(string))
				return new StringColumnAccessor(CompileSelector<string>(source, field.GetValue));
			var (hasValue, selector) = field.HasValue == null ? (null, field.Selector) : (CompileSelector<bool>(source, field.HasValue), field.GetValue);
			var createAccessor = Lambdas.CreateDelegate<Func<ParameterExpression, Expression, Func<T, bool>, IColumnAccessor>>(
				MakeAccessorMethod.MakeGenericMethod(selector.Type));
			return createAccessor(source, selector, hasValue);
		}

		static readonly MethodInfo MakeAccessorMethod = typeof(SequenceDataReader<T>)
			.GetMethod(nameof(MakeAccessorT), BindingFlags.Static | BindingFlags.NonPublic);

		static IColumnAccessor MakeAccessorT<TFieldType>(ParameterExpression source, Expression selector, Func<T, bool> hasValue) =>
			new ColumnAccessor<TFieldType>(CompileSelector<TFieldType>(source, selector), hasValue);

		static Func<T, TResult> CompileSelector<TResult>(ParameterExpression source, Expression selector) =>
			Expression.Lambda<Func<T, TResult>>(selector, source).Compile();

		public override object this[int i] => GetValue(i);
		public override object this[string name] => GetValue(GetOrdinal(name));

		public override int FieldCount => accessors.Count;

		public override int Depth => throw new NotSupportedException();
		public override bool HasRows => throw new NotSupportedException();
		public override bool IsClosed => false;
		public override int RecordsAffected => throw new NotSupportedException();

		public override bool Read() { 
			if(!data.MoveNext())
				return false;
			accessors = accessors.GetAccessors();
			return true;
		}

		public override bool NextResult() => false;

		public override void Close() { }

		protected override void Dispose(bool disposing) {
			if(disposing)
				data.Dispose();
		}

		public override DataTable GetSchemaTable() => schema.ToDataTable();
		public override string GetDataTypeName(int i) => schema[i].DataTypeName;
		public override Type GetFieldType(int i) => schema[i].ColumnType;
		public override string GetName(int i) => schema[i].ColumnName;
		public override int GetOrdinal(string name) {
			var o = schema.GetOrdinal(name);
			if (o < 0)
				throw new InvalidOperationException($"No field named '{name}' mapped");
			return o;
		}

		public override int GetValues(object[] values) {
			var n = Math.Min(FieldCount, values.Length);
			for (var i = 0; i != n; ++i)
				values[i] = GetValue(i);
			return n;
		}


		public override bool IsDBNull(int i) => accessors[i].IsDBNull(data.Current);
		public override object GetValue(int i) => accessors[i].GetValue(data.Current);
		public override TValue GetFieldValue<TValue>(int i) => accessors[i].GetFieldValue<TValue>(data.Current);

		public override bool GetBoolean(int i) => GetFieldValue<bool>(i);
		public override byte GetByte(int i) => GetFieldValue<byte>(i);
		public override char GetChar(int i) => GetFieldValue<char>(i);
		public override Guid GetGuid(int i) => GetFieldValue<Guid>(i);
		public override short GetInt16(int i) => GetFieldValue<short>(i);
		public override int GetInt32(int i) => GetFieldValue<int>(i);
		public override long GetInt64(int i) => GetFieldValue<long>(i);
		public override float GetFloat(int i) => GetFieldValue<float>(i);
		public override double GetDouble(int i) => GetFieldValue<double>(i);
		public override string GetString(int i) => GetFieldValue<string>(i);
		public override decimal GetDecimal(int i) => GetFieldValue<decimal>(i);
		public override DateTime GetDateTime(int i) => GetFieldValue<DateTime>(i);

		public override long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length) => throw new NotImplementedException();
		public override long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length) => throw new NotImplementedException();

		public IDataRecord GetRecord() => new DataRecord(this, data.Current);

		public override IEnumerator GetEnumerator() {
			while (Read())
				yield return this;
		}
	}
}
