using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;

namespace DataBoss.Data
{
	public static class SequenceDataReader
	{
		public static DbDataReader Items<T>(params T[] data) => Create(data);

		public static DbDataReader Create<T>(IEnumerable<T> data) => Create(data, x => x.MapAll());
		public static DbDataReader Create<T>(IEnumerable<T> data, Action<FieldMapping<T>> mapFields) => Create(data?.GetEnumerator(), mapFields);

		public static DbDataReader Create<T>(IEnumerator<T> data) => Create(data, x => x.MapAll());
		public static DbDataReader Create<T>(IEnumerator<T> data, Action<FieldMapping<T>> mapFields) {
			var fieldMapping = new FieldMapping<T>();
			mapFields(fieldMapping);
			return new SequenceDataReader<T>(data, fieldMapping);
		}

		public static DbDataReader Create<T>(IAsyncEnumerable<T> data) => Create(data, x => x.MapAll());
		public static DbDataReader Create<T>(IAsyncEnumerable<T> data, Action<FieldMapping<T>> mapFields) => Create(data?.GetAsyncEnumerator(), mapFields);

		public static DbDataReader Create<T>(IAsyncEnumerator<T> data) => Create(data, x => x.MapAll());
		public static DbDataReader Create<T>(IAsyncEnumerator<T> data, Action<FieldMapping<T>> mapFields) {
			var fieldMapping = new FieldMapping<T>();
			mapFields(fieldMapping);
			return new AsyncSequenceDataReader<T>(data, fieldMapping);
		}

		public static DbDataReader Create<T>(IEnumerable<T> data, params string[] members) =>
			Create(data, fields => Array.ForEach(members, x => fields.Map(x)));

		public static DbDataReader Create<T>(IEnumerable<T> data, params MemberInfo[] members) =>
			Create(data, fields => Array.ForEach(members, x => fields.Map(x)));

		public static DbDataReader ToDataReader<T>(this IEnumerable<T> data) => Create(data); 
	}

	public abstract class SequenceDataReaderBase<T> : DbDataReader, IDataRecordReader
	{
		abstract class FieldAccessor
		{
			public abstract bool IsDBNull(T item);
			public abstract object GetValue(T item);
			public abstract TValue GetFieldValue<TValue>(T item);

			public static FieldAccessor Create<TField>(Func<T, TField> get) => new ValueAccessor<TField>(get);
			public static FieldAccessor Create<TField>(Func<T, TField> get, Func<T, bool> hasValue) =>
				hasValue == null ? Create(get) : new NullableAccessor<TField>(get, hasValue);
		}

		abstract class FieldAccessor<TField> : FieldAccessor
		{
			readonly Func<T, TField> get;

			public FieldAccessor(Func<T, TField> get) {
				this.get = get;
			}

			public override TValue GetFieldValue<TValue>(T item) {
				if (typeof(TValue) == typeof(TField) || typeof(TValue) == typeof(object))
					return (TValue)(object)get(item);
				ThrowInvalidCastException<TField, TValue>();
				return default;
			}

			protected TField GetFieldValue(T item) => get(item);
		}

		sealed class ValueAccessor<TField> : FieldAccessor<TField>
		{
			public ValueAccessor(Func<T, TField> get) : base(get) { }

			public override object GetValue(T item) => GetFieldValue(item);
			public override bool IsDBNull(T item) => false;
		}

		sealed class NullableAccessor<TField> : FieldAccessor<TField>
		{
			readonly Func<T, bool> hasValue;

			public NullableAccessor(Func<T, TField> get, Func<T, bool> hasValue) : base(get) {
				this.hasValue = hasValue ?? throw new ArgumentNullException(nameof(hasValue));
			}

			public override object GetValue(T item) => IsDBNull(item) ? DBNull.Value : GetFieldValue(item);
			public override bool IsDBNull(T item) => !hasValue(item);
		}

		sealed class StringAccessor : FieldAccessor<string>
		{
			public StringAccessor(Func<T, string> get) : base(get) { }

			public override object GetValue(T item) => (object)GetFieldValue(item) ?? DBNull.Value;
			public override bool IsDBNull(T item) => GetFieldValue(item) == null;
		}

		static void ThrowInvalidCastException<TField, TValue>() => 
			throw new InvalidCastException($"Unable to cast object of type '{typeof(TField)}' to {typeof(TValue)}.");

		class DataRecord : IDataRecord
		{
			readonly DataReaderSchemaTable schema;
			readonly FieldAccessor[] fields;
			readonly T item;

			public DataRecord(DataReaderSchemaTable schema, FieldAccessor[] fields, T item) {
				this.schema = schema;
				this.fields = fields;
				this.item = item;
			}

			public int FieldCount => fields.Length;

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

			FieldAccessor GetAccessor(int i) => fields[i];

			public int GetValues(object[] values) {
				var n = Math.Min(FieldCount, values.Length);
				for (var i = 0; i != n; ++i)
					values[i] = GetValue(i);
				return n;
			}

			public object this[int i] => GetValue(i);
			public object this[string name] => GetValue(GetOrdinal(name));

			public long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferOffset, int length) => this.GetArray(i, fieldOffset, buffer, bufferOffset, length);
			public long GetChars(int i, long fieldOffset, char[] buffer, int bufferOffset, int length) => this.GetArray(i, fieldOffset, buffer, bufferOffset, length);

			public IDataReader GetData(int i) => throw new NotImplementedException();

			public string GetDataTypeName(int i) => schema[i].DataTypeName;
			public Type GetFieldType(int i) => schema[i].DataType;
			public string GetName(int i) => schema[i].ColumnName;
			public int GetOrdinal(string name) => schema.GetOrdinal(name);
		}

		readonly FieldAccessor[] fields;
		readonly DataReaderSchemaTable schema;
		bool hasData;

		internal SequenceDataReaderBase(FieldMapping<T> fields) {
			this.schema = GetSchema(fields);
			this.fields = new FieldAccessor[fields.Count];
			for (var i = 0; i != this.fields.Length; ++i)
				this.fields[i] = MakeAccessor(fields.Source, fields[i]);
		}

		static DataReaderSchemaTable GetSchema(FieldMapping<T> mapping) {
			var schema = new DataReaderSchemaTable();
			for (var i = 0; i != mapping.Count; ++i) {
				var dbType = mapping.GetDbType(i);
				schema.Add(
					ordinal: i,
					name: mapping.GetFieldName(i),
					dataType: mapping.GetFieldType(i),
					allowDBNull: dbType.IsNullable,
					columnSize: dbType.ColumnSize,
					dataTypeName: dbType.TypeName);
			}
			return schema;
		}

		static FieldAccessor MakeAccessor(ParameterExpression source, in FieldMappingItem field) {
			if (field.FieldType == typeof(string))
				return new StringAccessor(CompileSelector<string>(source, field.GetValue));
			var (hasValue, selector) = field.HasValue == null ? (null, field.Selector) : (CompileSelector<bool>(source, field.HasValue), field.GetValue);
			var createAccessor = Lambdas.CreateDelegate<Func<ParameterExpression, Expression, Func<T, bool>, FieldAccessor>>(
				MakeAccessorMethod.MakeGenericMethod(selector.Type));
			return createAccessor(source, selector, hasValue);
		}

		static readonly MethodInfo MakeAccessorMethod = typeof(SequenceDataReaderBase<T>)
			.GetMethod(nameof(MakeAccessorT), BindingFlags.Static | BindingFlags.NonPublic);

		static FieldAccessor MakeAccessorT<TFieldType>(ParameterExpression source, Expression selector, Func<T, bool> hasValue) =>
			FieldAccessor.Create(CompileSelector<TFieldType>(source, selector), hasValue);

		static Func<T, TResult> CompileSelector<TResult>(ParameterExpression source, Expression selector) =>
			Expression.Lambda<Func<T, TResult>>(selector, source).Compile();

		public override object this[int i] => GetValue(i);
		public override object this[string name] => GetValue(GetOrdinal(name));

		public override int FieldCount => fields.Length;

		public override int Depth => throw new NotSupportedException();
		public override bool HasRows => throw new NotSupportedException();
		public override int RecordsAffected => throw new NotSupportedException();

		public sealed override bool Read() => hasData = DoRead();
		public sealed override async Task<bool> ReadAsync(CancellationToken cancellationToken) => hasData = await DoReadAsync(cancellationToken);

		protected abstract bool DoRead();
		protected abstract Task<bool> DoReadAsync(CancellationToken cancellationToken);

		public override bool NextResult() => false;

		protected override void Dispose(bool disposing) {
			if (disposing)
				Close();
		}

		public override DataTable GetSchemaTable() => schema.ToDataTable();
		public override string GetDataTypeName(int i) => schema[i].DataTypeName;
		public override Type GetFieldType(int i) => schema[i].DataType;
		public override string GetName(int i) => schema[i].ColumnName;
		public override int GetOrdinal(string name) => schema.GetOrdinal(name);

		public override int GetValues(object[] values) {
			var n = Math.Min(FieldCount, values.Length);
			for (var i = 0; i != n; ++i)
				values[i] = GetValue(i);
			return n;
		}

		public override bool IsDBNull(int i) => fields[i].IsDBNull(Current);
		public override object GetValue(int i) => fields[i].GetValue(Current);

		public override TValue GetFieldValue<TValue>(int i) => GetCurrentValue<TValue>(i);
		public override bool GetBoolean(int i) => GetCurrentValue<bool>(i);
		public override byte GetByte(int i) => GetCurrentValue<byte>(i);
		public override char GetChar(int i) => GetCurrentValue<char>(i);
		public override Guid GetGuid(int i) => GetCurrentValue<Guid>(i);
		public override short GetInt16(int i) => GetCurrentValue<short>(i);
		public override int GetInt32(int i) => GetCurrentValue<int>(i);
		public override long GetInt64(int i) => GetCurrentValue<long>(i);
		public override float GetFloat(int i) => GetCurrentValue<float>(i);
		public override double GetDouble(int i) => GetCurrentValue<double>(i);
		public override string GetString(int i) => GetCurrentValue<string>(i);
		public override decimal GetDecimal(int i) => GetCurrentValue<decimal>(i);
		public override DateTime GetDateTime(int i) => GetCurrentValue<DateTime>(i);

		TValue GetCurrentValue<TValue>(int i) => fields[i].GetFieldValue<TValue>(Current);
		T Current => hasData ? GetCurrent() : NoData();
		protected abstract T GetCurrent();
		static T NoData() => throw new InvalidOperationException("Invalid attempt to read when no data is present, call Read()");

		public override long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferOffset, int length) => this.GetArray(i, fieldOffset, buffer, bufferOffset, length);
		public override long GetChars(int i, long fieldOffset, char[] buffer, int bufferOffset, int length) => this.GetArray(i, fieldOffset, buffer, bufferOffset, length);

		public IDataRecord GetRecord() => new DataRecord(schema, fields, Current);

		public override IEnumerator GetEnumerator() {
			while (Read())
				yield return this;
		}
	}

	public sealed class SequenceDataReader<T> : SequenceDataReaderBase<T>
	{
		IEnumerator<T> data;

		internal SequenceDataReader(IEnumerator<T> data, FieldMapping<T> fields): base(fields)  {
			this.data = data ?? throw new ArgumentNullException(nameof(data));
		}

		public override void Close() {
			data?.Dispose();
			data = null;
		}

		public override bool IsClosed => data is null;

		protected override T GetCurrent() => data.Current;
		protected override bool DoRead() => data.MoveNext();
		protected override Task<bool> DoReadAsync(CancellationToken cancellationToken) => Task.FromResult(DoRead());
	}

	public sealed class AsyncSequenceDataReader<T> : SequenceDataReaderBase<T>
	{
		IAsyncEnumerator<T> data;

		internal AsyncSequenceDataReader(IAsyncEnumerator<T> data, FieldMapping<T> fields): base(fields)  {
			this.data = data ?? throw new ArgumentNullException(nameof(data));
		}

		public override bool IsClosed => data is null;

		public override void Close() {
			if(data != null) {
				var x = data.DisposeAsync();
				if(x.IsCompleted)
					x.GetAwaiter().GetResult();
				else x.AsTask().ConfigureAwait(false).GetAwaiter().GetResult();

			}
			data = null;
		}

		protected override T GetCurrent() => data.Current;

		protected override async Task<bool> DoReadAsync(CancellationToken cancellationToken) => await data.MoveNextAsync();

		protected override bool DoRead() {
			var x = data.MoveNextAsync();
			return x.IsCompleted
			? x.GetAwaiter().GetResult()
			: x.AsTask().ConfigureAwait(false).GetAwaiter().GetResult();
		}
	}

}
