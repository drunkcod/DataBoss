using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Linq.Expressions;
using DataBoss.Data;

namespace DataBoss.DataPackage
{
	public class DataReaderTransform : IDataReader
	{
		delegate IFieldAccessor AccessorBuilder(int ordinal, bool allowDBNull, int columnSize);
		static readonly ConcurrentDictionary<Type, AccessorBuilder> fieldAccessorCtorCache = new();

		static IFieldAccessor CreateFieldAccessor(int ordinal, bool isNullable, int columnSize, Type type) {
			var ctor = fieldAccessorCtorCache.GetOrAdd(type, fieldType => {
				var ps = Array.ConvertAll(
					typeof(AccessorBuilder).GetMethod("Invoke").GetParameters(),
					x => Expression.Parameter(x.ParameterType, x.Name));
				var c = typeof(FieldAccessor<>).MakeGenericType(fieldType).GetConstructor(Array.ConvertAll(ps, x => x.Type));
				return Expression.Lambda<AccessorBuilder>(Expression.New(c, ps), ps).Compile();
			});

			return ctor(ordinal, isNullable, columnSize);
		}

		interface IFieldAccessor
		{
			bool AllowDBNull { get; }
			int ColumnSize { get; }
			Type FieldType { get; }

			bool IsDBNull(IDataRecord record);

			object GetValue(IDataRecord record);
			T GetFieldValue<T>(IDataRecord record);
		}

		class FieldAccessor<TField> : IFieldAccessor
		{
			readonly int ordinal;

			public bool AllowDBNull { get; }
			public int ColumnSize { get; }
			public Type FieldType => FieldInfo<TField>.FieldType;

			public FieldAccessor(int ordinal, bool allowDBNull, int columnSize) {
				this.ordinal = ordinal;
				this.AllowDBNull = allowDBNull;
				this.ColumnSize = columnSize;
			}

			public bool IsDBNull(IDataRecord record) => record.IsDBNull(ordinal);

			public object GetValue(IDataRecord record) => GetFieldValue<TField>(record);
			public T GetFieldValue<T>(IDataRecord record) => record.GetFieldValue<T>(ordinal);
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

			public T GetFieldValue<T>(IDataRecord record) =>
				(T)(object)GetRecordValue(record);


			public object GetValue(IDataRecord record) {
				var value = GetRecordValue(record);
				return FieldInfo<TField>.IsNull(value) ? DBNull.Value : value;
			}

			public bool IsDBNull(IDataRecord record) => AllowDBNull && FieldInfo<TField>.IsNull(GetRecordValue(record));

			TField GetRecordValue(IDataRecord record) {
				if (isDirty) {
					currentValue = GetUserValue(record);
					isDirty = false;
				}
				return currentValue;
			}

			protected abstract TField GetUserValue(IDataRecord record);

			public void Dirty() { isDirty = true; }
		}

		class FieldTransform<T> : UserFieldAccessor<T>
		{
			readonly Func<IDataRecord, T> transform;

			public FieldTransform(Func<IDataRecord, T> transform) {
				this.transform = transform;
			}

			protected override T GetUserValue(IDataRecord record) => transform(record);
		}

		class FieldTransform<TField, T> : UserFieldAccessor<T>
		{
			readonly Func<TField, T> transform;
			readonly int ordinal;
			readonly bool allowDBNull;

			public FieldTransform(int ordinal, Func<TField, T> transform, bool allowDBNull) {
				this.transform = transform;
				this.allowDBNull = allowDBNull;
				this.ordinal = ordinal;
			}

			public override bool AllowDBNull => allowDBNull;

			protected override T GetUserValue(IDataRecord record) => transform(record.GetFieldValue<TField>(ordinal));
		}

		static class FieldInfo<T>
		{
			public static readonly Type FieldType = typeof(T).TryGetNullableTargetType(out var targetType) ? targetType : typeof(T);
			public static readonly bool IsNullable = typeof(T).IsNullable();
			public static readonly int ColumnSize = DataBossDbType.From(typeof(T)).ColumnSize ?? -1;
			public static readonly Func<T, bool> IsNull = IsNullable 
				? CreateIsNullMethod()
				: _ => false;

			static Func<T, bool> CreateIsNullMethod() {
				var p0 = Expression.Parameter(typeof(T));

				return Expression.Lambda<Func<T, bool>>(
					Expression.Not(Expression.MakeMemberAccess(p0, typeof(T).GetMember("HasValue").Single())), p0)
					.Compile();
			}
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

		public DataReaderTransform Add<T>(string name, Func<IDataRecord, T> getValue) {
			var newField = new FieldTransform<T>(getValue);
			onRead += newField.Dirty;
			fields.Add((newField, name));
			return this;
		}

		public DataReaderTransform Remove(string name) {
			fields.RemoveAll(x => x.Name == name);
			return this;
		}

		public DataReaderTransform Transform<T>(string name, Func<IDataRecord, T> transform) {
			var o = inner.GetOrdinal(name);
			var newField = new FieldTransform<T>(transform);
			fields[o] = (newField, name);
			onRead += newField.Dirty;
			return this;
		}

		public DataReaderTransform Transform<TField, T>(string name, Func<TField, T> transform) =>
			Transform(name, inner.GetOrdinal(name), transform);
			
		public DataReaderTransform Transform<TField, T>(int ordinal, Func<TField, T> transform) =>
			Transform(inner.GetName(ordinal), ordinal, transform);

		DataReaderTransform Transform<TField, T>(string name, int ordinal, Func<TField, T> transform) {
			var newField = new FieldTransform<TField, T>(ordinal, transform, 
				FieldInfo<TField>.IsNullable ? FieldInfo<T>.IsNullable : FieldInfo<T>.IsNullable || fields[ordinal].Accessor.AllowDBNull);
			fields[ordinal] = (newField, name);
			return this;
		}

		public DataReaderTransform Set<T>(string name, Func<IDataRecord, T> getValue) {
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

		public T GetFieldValue<T>(int i) => fields[i].Accessor.GetFieldValue<T>(inner);
		public object GetValue(int i) => fields[i].Accessor.GetValue(inner);

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

		public bool IsDBNull(int i) => fields[i].Accessor.IsDBNull(inner);
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
