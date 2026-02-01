using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace DataBoss.Data;

public static class SequenceDataReader
{
	public static DbDataReader Items<T>(params T[] data) => Create(data);

	public static DbDataReader Create<T>(IEnumerable<T> data) => SequenceDataReaderBase<T>.CreateDefault(data);
	public static DbDataReader Create<T>(IEnumerable<T> data, Action<FieldMapping<T>> mapFields) => Create(data?.GetEnumerator(), mapFields);

	public static DbDataReader Create<T>(IEnumerator<T> data) => SequenceDataReaderBase<T>.CreateDefault(data);
	public static DbDataReader Create<T>(IEnumerator<T> data, Action<FieldMapping<T>> mapFields) {
		var fieldMapping = new FieldMapping<T>();
		mapFields(fieldMapping);
		return new SequenceDataReader<T>(data, fieldMapping);
	}

	public static DbDataReader Create<T>(IAsyncEnumerable<T> data) => SequenceDataReaderBase<T>.CreateDefault(data);
	public static DbDataReader Create<T>(IAsyncEnumerable<T> data, Action<FieldMapping<T>> mapFields) => Create(data?.GetAsyncEnumerator(), mapFields);

	public static DbDataReader Create<T>(IAsyncEnumerator<T> data) => SequenceDataReaderBase<T>.CreateDefault(data);
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
	internal readonly struct FieldConfiguration(DataReaderSchemaTable schema, FieldAccessor<T>[] fields)
	{
		public readonly DataReaderSchemaTable Schema = schema;
		public readonly FieldAccessor<T>[] Fields = fields;

		public static readonly FieldConfiguration Default = CreateDefault();
		static FieldConfiguration CreateDefault() {
			var fields = new FieldMapping<T>();
			fields.MapAll();
			return new FieldConfiguration(GetSchema(fields), MakeAccessors(fields));
		}
	}

	public static DbDataReader CreateDefault(IEnumerable<T> data) => new SequenceDataReader<T>(data?.GetEnumerator(), FieldConfiguration.Default);
	public static DbDataReader CreateDefault(IEnumerator<T> data) => new SequenceDataReader<T>(data, FieldConfiguration.Default);
	public static DbDataReader CreateDefault(IAsyncEnumerable<T> data) => CreateDefault(data?.GetAsyncEnumerator());
	public static DbDataReader CreateDefault(IAsyncEnumerator<T> data) => new AsyncSequenceDataReader<T>(data, FieldConfiguration.Default);

	class DataRecord(DataReaderSchemaTable schema, FieldAccessor<T>[] fields, T item) : IDataRecord2
	{
		void IDisposable.Dispose() { }

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

		FieldAccessor<T> GetAccessor(int i) => fields[i];

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

	readonly FieldAccessor<T>[] fields;
	readonly DataReaderSchemaTable schema;
	bool hasData;

	internal SequenceDataReaderBase(FieldMapping<T> fields) : this(GetSchema(fields), MakeAccessors(fields)) { }

	internal SequenceDataReaderBase(DataReaderSchemaTable schema, FieldAccessor<T>[] fields) {
		this.schema = schema;
		this.fields = fields;
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

	static FieldAccessor<T>[] MakeAccessors(FieldMapping<T> fields) {
		var accessors = new FieldAccessor<T>[fields.Count];
		for (var i = 0; i != accessors.Length; ++i) {
			var field = fields[i];
			var (hasValue, selector) = field.HasValue == null ? (null, field.Selector) : (field.HasValue, field.GetValue);
			accessors[i] = FieldAccessor<T>.Create(fields.Source, selector, hasValue);
		}
		return accessors;
	}

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

	public IDataRecord2 GetRecord() => new DataRecord(schema, fields, Current);

	public override IEnumerator GetEnumerator() {
		while (Read())
			yield return this;
	}
}

public sealed class SequenceDataReader<T> : SequenceDataReaderBase<T>
{
	IEnumerator<T> data;

	internal SequenceDataReader(IEnumerator<T> data, FieldMapping<T> fields) : base(fields) {
		this.data = data ?? throw new ArgumentNullException(nameof(data));
	}

	internal SequenceDataReader(IEnumerator<T> data, FieldConfiguration config) : this(data, config.Schema, config.Fields) { }
	internal SequenceDataReader(IEnumerator<T> data, DataReaderSchemaTable schema, FieldAccessor<T>[] fields) : base(schema, fields) {
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

	internal AsyncSequenceDataReader(IAsyncEnumerator<T> data, FieldMapping<T> fields) : base(fields) {
		this.data = data ?? throw new ArgumentNullException(nameof(data));
	}

	internal AsyncSequenceDataReader(IAsyncEnumerator<T> data, FieldConfiguration config) : this(data, config.Schema, config.Fields) { }
	internal AsyncSequenceDataReader(IAsyncEnumerator<T> data, DataReaderSchemaTable schema, FieldAccessor<T>[] fields) : base(schema, fields) {
		this.data = data ?? throw new ArgumentNullException(nameof(data));
	}

	public override bool IsClosed => data is null;

	public override void Close() {
		if (data != null) Sync.GetResult(data.DisposeAsync());
		data = null;
	}

	protected override T GetCurrent() => data.Current;

	protected override async Task<bool> DoReadAsync(CancellationToken cancellationToken) => await data.MoveNextAsync();

	protected override bool DoRead() => Sync.GetResult(data.MoveNextAsync());
}
