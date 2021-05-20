using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Linq.Expressions;
using DataBoss.Data;

namespace DataBoss.DataPackage
{
	public interface ITransformedDataRecord : IDataRecord
	{
		IDataRecord Source { get; }
	}

	public class DataReaderTransform : IDataReader, ITransformedDataRecord
	{
		public static class FieldInfo<T>
		{
			public static readonly Type FieldType = typeof(T).TryGetNullableTargetType(out var targetType) ? targetType : typeof(T);
			public static readonly bool IsNullable = typeof(T).IsNullable();
			public static readonly int ColumnSize = DataBossDbType.From(typeof(T)).ColumnSize ?? -1;
			public static readonly Func<T, bool> IsNull = CreateIsNullMethod();

			static Func<T, bool> CreateIsNullMethod() {
				var p0 = Expression.Parameter(typeof(T));
				Expression CreateNullCheck(Expression input) {
					if (p0.Type.IsNullable())
						return Expression.Not(Expression.MakeMemberAccess(input, typeof(T).GetMember("HasValue").Single()));
					if (p0.Type.IsValueType)
						return Expression.Constant(false, typeof(bool));

					return Expression.ReferenceEqual(input, Expression.Constant(null, p0.Type));
				}
				return Expression.Lambda<Func<T, bool>>(CreateNullCheck(p0), p0).Compile();
			}
		}

		delegate IFieldAccessor AccessorBuilder(int ordinal, bool allowDBNull, int columnSize);

		static readonly ConcurrentDictionary<Type, AccessorBuilder> fieldAccessorCtorCache = new();

		static IFieldAccessor CreateFieldAccessor(int ordinal, bool isNullable, int columnSize, Type type) {
			var ctor = fieldAccessorCtorCache.GetOrAdd(type, fieldType => {
				var ps = Array.ConvertAll(
					typeof(AccessorBuilder).GetMethod("Invoke").GetParameters(),
					x => Expression.Parameter(x.ParameterType, x.Name));
				var c = typeof(SourceFieldAccessor<>).MakeGenericType(fieldType).GetConstructor(Array.ConvertAll(ps, x => x.Type));
				return Expression.Lambda<AccessorBuilder>(Expression.New(c, ps), ps).Compile();
			});

			return ctor(ordinal, isNullable, columnSize);
		}

		interface IFieldAccessor
		{
			bool AllowDBNull { get; }
			int ColumnSize { get; }
			Type FieldType { get; }

			bool IsDBNull(DataReaderTransform record);

			object GetValue(DataReaderTransform record);
			T GetFieldValue<T>(DataReaderTransform record);
		}

		abstract class SourceFieldAccessor : IFieldAccessor
		{
			public SourceFieldAccessor(int ordinal, bool allowDBNull, int columnSize) {
				this.Ordinal = ordinal;
				this.AllowDBNull = allowDBNull;
				this.ColumnSize = columnSize;
			}

			public bool AllowDBNull { get; }
			public int ColumnSize { get; }
			public int Ordinal { get; }
			public abstract Type FieldType { get; }

			public bool IsDBNull(DataReaderTransform record) => record.Source.IsDBNull(Ordinal);

			public abstract object GetValue(DataReaderTransform record);
			public T GetFieldValue<T>(DataReaderTransform record) => record.Source.GetFieldValue<T>(Ordinal);
		}

		class SourceFieldAccessor<TField> : SourceFieldAccessor
		{
			public SourceFieldAccessor(int ordinal, bool allowDBNull, int columnSize) : base(ordinal, allowDBNull, columnSize) 
			{ }

			public override Type FieldType => FieldInfo<TField>.FieldType;
			public override object GetValue(DataReaderTransform record) => GetFieldValue<TField>(record);
		}

		abstract class UserFieldAccessor<TField> : IFieldAccessor
		{
			TField currentValue;
			bool isDirty;

			public UserFieldAccessor() {
				this.isDirty = true;
			}

			public virtual bool AllowDBNull => FieldInfo<TField>.IsNullable;
			public int ColumnSize => FieldInfo<TField>.ColumnSize;
			public Type FieldType => FieldInfo<TField>.FieldType;

			public T GetFieldValue<T>(DataReaderTransform record) =>
				(T)(object)GetCurrentValue(record);

			public object GetValue(DataReaderTransform record) {
				var value = GetCurrentValue(record);
				return FieldInfo<TField>.IsNull(value) ? DBNull.Value : value;
			}

			public bool IsDBNull(DataReaderTransform record) => AllowDBNull && FieldInfo<TField>.IsNull(GetCurrentValue(record));

			TField GetCurrentValue(DataReaderTransform record) {
				if (isDirty) {
					currentValue = GetRecordValue(record);
					isDirty = false;
				}
				return currentValue;
			}

			protected abstract TField GetRecordValue(DataReaderTransform record);

			public void Dirty() { isDirty = true; }
		}

		class FieldTransform<T> : UserFieldAccessor<T>
		{
			readonly Func<ITransformedDataRecord, T> transform;

			public FieldTransform(Func<ITransformedDataRecord, T> transform) {
				this.transform = transform;
			}

			protected override T GetRecordValue(DataReaderTransform record) => transform(record);
		}

		class FieldTransform<TField, T> : UserFieldAccessor<T>
		{
			readonly IFieldAccessor source;
			readonly Func<TField, T> transform;
			readonly bool allowDBNull;

			public FieldTransform(IFieldAccessor source, Func<TField, T> transform, bool allowDBNull) {
				this.source = source;
				this.transform = transform;
				this.allowDBNull = allowDBNull;
			}

			public override bool AllowDBNull => allowDBNull;

			protected override T GetRecordValue(DataReaderTransform record) => transform(source.GetFieldValue<TField>(record));
		}

		class RecordTransform<TRecord, T> : UserFieldAccessor<T>
		{
			Func<IDataReader, TRecord> readRecord;
			readonly Func<TRecord, T> selector;

			public string Name;

			public RecordTransform(Func<TRecord, T> selector) {
				this.selector = selector;
			}

			protected override T GetRecordValue(DataReaderTransform record) =>
				selector(ReadRecord(record));

			TRecord ReadRecord(IDataReader reader) =>
				(readRecord ??= ConverterFactory.Default.GetConverter<IDataReader, TRecord>(reader).Compiled)(reader);

		}

		readonly IDataReader inner;
		readonly List<(IFieldAccessor Accessor, string Name)> fields = new();
		Action onRead;

		public DataReaderTransform(IDataReader inner) {
			this.inner = inner;
			var schema = inner.GetDataReaderSchemaTable();
			var sourceFields = new (IFieldAccessor, string)[schema.Count];
			for(var i = 0; i != schema.Count; ++i) {
				var field = schema[i];
				sourceFields[field.Ordinal] = (CreateFieldAccessor(
					field.Ordinal,
					field.AllowDBNull, 
					field.ColumnSize ?? -1, 
					field.ColumnType), field.ColumnName);
			}
			this.fields.AddRange(sourceFields);
		}

		public IDataRecord Source => inner;

		public DataReaderTransform Add<T>(string name, Func<ITransformedDataRecord, T> getValue) {
			fields.Add(Bind(new FieldTransform<T>(getValue), name));
			return this;
		}

		public DataReaderTransform Add<T>(int ordinal, string name, Func<ITransformedDataRecord, T> getValue) {
			fields.Insert(ordinal, Bind(new FieldTransform<T>(getValue), name));
			return this;
		}

		public DataReaderTransform Add<TRecord, T>(int ordinal, string name, Func<TRecord, T> selector) {
			fields.Insert(ordinal, Bind(new RecordTransform<TRecord, T>(selector) { Name = name }, name));
			return this;
		}

		public DataReaderTransform Remove(string name) {
			fields.RemoveAll(x => x.Name == name);
			return this;
		}

		public DataReaderTransform Transform<T>(string name, Func<ITransformedDataRecord, T> transform) {
			fields[GetOrdinal(name)] = Bind(new FieldTransform<T>(transform), name);
			return this;
		}

		public DataReaderTransform Transform<TField, T>(string name, Func<TField, T> transform) {
			var o = GetOrdinal(name);
			return Transform(name, o, fields[o].Accessor, transform);
		}

		public DataReaderTransform Transform<TField, T>(int ordinal, Func<TField, T> transform) {
			return Transform(GetName(ordinal), ordinal, fields[ordinal].Accessor, transform);
		}

		DataReaderTransform Transform<TField, T>(string name, int ordinal, IFieldAccessor source, Func<TField, T> transform) {
			var allowDBNull = 
				FieldInfo<TField>.IsNullable 
				? FieldInfo<T>.IsNullable 
				: FieldInfo<T>.IsNullable || source.AllowDBNull;
			
			fields[ordinal] = Bind(new FieldTransform<TField, T>(source, transform, allowDBNull), name);
			return this;
		}

		(IFieldAccessor, string) Bind<T>(UserFieldAccessor<T> accessor, string name) {
			onRead += accessor.Dirty;
			return (accessor, name);
		}

		public DataReaderTransform Set<T>(string name, Func<ITransformedDataRecord, T> getValue) {
			var n = fields.FindIndex(x => x.Name == name);
			return n == -1
			? Add(name, getValue)
			: Transform(name, getValue);
		}

		public DataReaderTransform Rename(string from, string to) {
			var o = GetOrdinal(from);
			fields[o] = (fields[o].Accessor, to);
			return this;
		}

		public T GetFieldValue<T>(int i) => fields[i].Accessor.GetFieldValue<T>(this);
		public object GetValue(int i) => fields[i].Accessor.GetValue(this);

		object IDataRecord.this[int i] => GetValue(i);
		object IDataRecord.this[string name] => GetValue(GetOrdinal(name));
		object IDataRecord.GetValue(int i) => GetValue(i);

		int IDataReader.Depth => inner.Depth;
		bool IDataReader.IsClosed => inner.IsClosed;
		int IDataReader.RecordsAffected => inner.RecordsAffected;
		public int FieldCount => fields.Count;

		void IDataReader.Close() => inner.Close();
		void IDisposable.Dispose() => inner.Dispose();

		public Type GetFieldType(int i) => fields[i].Accessor.FieldType;
		bool IDataRecord.GetBoolean(int i) => GetFieldValue<bool>(i);
		byte IDataRecord.GetByte(int i) => GetFieldValue<byte>(i);
		char IDataRecord.GetChar(int i) => GetFieldValue<char>(i);
		DateTime IDataRecord.GetDateTime(int i) => GetFieldValue<DateTime>(i);
		decimal IDataRecord.GetDecimal(int i) => GetFieldValue<decimal>(i);
		double IDataRecord.GetDouble(int i) => GetFieldValue<double>(i);
		float IDataRecord.GetFloat(int i) => GetFieldValue<float>(i);
		Guid IDataRecord.GetGuid(int i) => GetFieldValue<Guid>(i);
		short IDataRecord.GetInt16(int i) => GetFieldValue<short>(i);
		int IDataRecord.GetInt32(int i) => GetFieldValue<int>(i);
		long IDataRecord.GetInt64(int i) => GetFieldValue<long>(i);
		string IDataRecord.GetString(int i) => GetFieldValue<string>(i);

		int IDataRecord.GetValues(object[] values) {
			var n = Math.Min(FieldCount, values.Length);
			for (var i = 0; i != n; ++i)
				values[i] = GetValue(i);
			return n;
		}

		public string GetName(int i) => fields[i].Name;
		public int GetOrdinal(string name) => fields.FindIndex(x => x.Name == name);

		DataTable IDataReader.GetSchemaTable() {
			var schema = new DataReaderSchemaTable();
			for (var i = 0; i != FieldCount; ++i) {
				var (item, name) = fields[i];
				schema.Add(name, i, item.FieldType, item.AllowDBNull, item.ColumnSize);
			}
			return schema.ToDataTable();
		}

		string IDataRecord.GetDataTypeName(int i) => DataBossDbType.From(GetFieldType(i)).TypeName;

		public bool IsDBNull(int i) => fields[i].Accessor.IsDBNull(this);
		bool IDataReader.NextResult() => inner.NextResult();
		bool IDataReader.Read() {
			onRead?.Invoke();
			return inner.Read();
		}

		long IDataRecord.GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length) => throw new NotSupportedException();
		long IDataRecord.GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length) => throw new NotSupportedException();
		IDataReader IDataRecord.GetData(int i) => throw new NotSupportedException();
	}

	public static class DataReaderTransformExtensions
	{
		public static IDataReader WithTransform(this IDataReader self, Action<DataReaderTransform> defineTransfrom) {
			var r = new DataReaderTransform(self);
			defineTransfrom(r);
			return r;
		}

		public static DataReaderTransform SpecifyDateTimeKind(this DataReaderTransform self, DateTimeKind kind) {
			for(var i = 0; i != self.FieldCount; ++i) {
				if (self.GetFieldType(i) == typeof(DateTime))
					self.Transform(i, (DateTime x) => DateTime.SpecifyKind(x, kind));
			}

			return self;
		}
	}
}
