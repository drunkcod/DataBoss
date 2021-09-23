using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Linq.Expressions;
using DataBoss.Data;

namespace DataBoss.DataPackage
{
	public class DataReaderTransform : DbDataReader, ITransformedDataRecord
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

		static readonly ConcurrentDictionary<(Type Type, Type ProviderSpecificType), AccessorBuilder> fieldAccessorCtorCache = new();

		static IFieldAccessor CreateFieldAccessor(int ordinal, bool isNullable, int columnSize, Type type, Type providerSpecificType) {
			var ctor = fieldAccessorCtorCache.GetOrAdd((type, providerSpecificType), key => {
				var ps = Array.ConvertAll(
					typeof(AccessorBuilder).GetMethod("Invoke").GetParameters(),
					x => Expression.Parameter(x.ParameterType, x.Name));
				var c = typeof(SourceFieldAccessor<,>).MakeGenericType(key.Type, key.ProviderSpecificType).GetConstructor(Array.ConvertAll(ps, x => x.Type));
				return Expression.Lambda<AccessorBuilder>(Expression.New(c, ps), ps).Compile();
			});

			return ctor(ordinal, isNullable, columnSize);
		}

		interface IFieldAccessor
		{
			bool AllowDBNull { get; }
			int ColumnSize { get; }
			Type FieldType { get; }
			Type ProviderSpecificFieldType { get; }
			string Name { get; set; }

			bool IsDBNull(DataReaderTransform record);

			object GetValue(DataReaderTransform record);
			object GetProviderSpecificValue(DataReaderTransform record);
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
			public abstract Type ProviderSpecificFieldType { get; }
			public string Name { get; set; }

			public bool IsDBNull(DataReaderTransform record) => record.inner.IsDBNull(Ordinal);

			public abstract object GetValue(DataReaderTransform record);
			public abstract object GetProviderSpecificValue(DataReaderTransform record);
			public T GetFieldValue<T>(DataReaderTransform record) => record.inner.GetFieldValue<T>(Ordinal);
		}

		class SourceFieldAccessor<TField, TProviderType> : SourceFieldAccessor
		{
			public SourceFieldAccessor(int ordinal, bool allowDBNull, int columnSize) : base(ordinal, allowDBNull, columnSize) 
			{ }

			public override Type FieldType => FieldInfo<TField>.FieldType;
			public override Type ProviderSpecificFieldType => typeof(TProviderType);
			public override object GetValue(DataReaderTransform record) => GetFieldValue<TField>(record);
			public override object GetProviderSpecificValue(DataReaderTransform record) => GetFieldValue<TProviderType>(record);
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
			public Type ProviderSpecificFieldType => FieldType;
			public string Name { get; set; }

			public T GetFieldValue<T>(DataReaderTransform record) =>
				(T)(object)GetCurrentValue(record);

			public object GetValue(DataReaderTransform record) {
				var value = GetCurrentValue(record);
				return FieldInfo<TField>.IsNull(value) ? DBNull.Value : value;
			}

			public object GetProviderSpecificValue(DataReaderTransform record) => GetValue(record);

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

		interface IRecordTransform 
		{
			void Reset();
		}

		class RecordTransform<TRecord, T> : UserFieldAccessor<T>, IRecordTransform
		{
			Func<IDataReader, TRecord> readRecord;
			readonly Func<TRecord, T> selector;

			public RecordTransform(Func<TRecord, T> selector) {
				this.selector = selector;
			}

			protected override T GetRecordValue(DataReaderTransform record) =>
				selector(ReadRecord(record));

			TRecord ReadRecord(DataReaderTransform reader) {
				if(readRecord == null) {
					var self = reader.fields.IndexOf(this);
					var fields = FieldMap.Create(reader.AsDbDataReader(), x => x.Ordinal < self || reader.fields[x.Ordinal] is SourceFieldAccessor);
					readRecord = ConverterFactory.Default.BuildConverter<IDataReader, TRecord>(fields).Compiled;
				}
				return readRecord(reader);
			}

			void IRecordTransform.Reset() {
				readRecord = null;
			}
		}

		readonly DbDataReader inner;
		readonly List<IFieldAccessor> fields = new();
		Action onRead;

		public DataReaderTransform(IDataReader inner) : this(inner.AsDbDataReader()) { }
		public DataReaderTransform(DbDataReader inner) {
			this.inner = inner;

			var schema = inner.GetDataReaderSchemaTable();
			var sourceFields = new IFieldAccessor[schema.Count];
			for(var i = 0; i != schema.Count; ++i) {
				var field = schema[i];
				var accessor = CreateFieldAccessor(
					field.Ordinal,
					field.AllowDBNull, 
					field.ColumnSize ?? -1, 
					field.DataType,
					field.ProviderSpecificDataType);
				accessor.Name = field.ColumnName;
				sourceFields[field.Ordinal] = accessor;
			}
			this.fields.AddRange(sourceFields);
		}

		public IDataRecord Source => inner;

		public DataReaderTransform Add<T>(string name, Func<ITransformedDataRecord, T> getValue) {
			fields.Add(Bind(new FieldTransform<T>(getValue) { Name = name }));
			return this;
		}

		public DataReaderTransform Add<T>(int ordinal, string name, Func<ITransformedDataRecord, T> getValue) {
			fields.Insert(ordinal, Bind(new FieldTransform<T>(getValue) { Name = name }));
			return this;
		}

		public DataReaderTransform Add<TRecord, T>(string name, Func<TRecord, T> selector) {
			fields.Add(Bind(new RecordTransform<TRecord, T>(selector) { Name = name }));
			return this;
		}

		public DataReaderTransform Add<TRecord, T>(int ordinal, string name, Func<TRecord, T> selector) {
			fields.Insert(ordinal, Bind(new RecordTransform<TRecord, T>(selector) { Name = name }));
			return this;
		}

		public DataReaderTransform Remove(string name) {
			fields.RemoveAll(x => x.Name == name);
			return this;
		}

		public DataReaderTransform Transform<T>(string name, Func<ITransformedDataRecord, T> transform) {
			fields[GetOrdinal(name)] = Bind(new FieldTransform<T>(transform) { Name = name });
			return this;
		}

		public DataReaderTransform Transform<TField, T>(string name, Func<TField, T> transform) {
			var o = GetOrdinal(name);
			return Transform(name, o, fields[o], transform);
		}

		public DataReaderTransform Transform<TField, T>(int ordinal, Func<TField, T> transform) =>
			Transform(GetName(ordinal), ordinal, fields[ordinal], transform);

		DataReaderTransform Transform<TField, T>(string name, int ordinal, IFieldAccessor source, Func<TField, T> transform) {
			var allowDBNull = 
				FieldInfo<TField>.IsNullable 
				? FieldInfo<T>.IsNullable 
				: FieldInfo<T>.IsNullable || source.AllowDBNull;
			
			fields[ordinal] = Bind(new FieldTransform<TField, T>(source, transform, allowDBNull) { Name = name });
			return this;
		}

		IFieldAccessor Bind<T>(UserFieldAccessor<T> accessor) {
			onRead += accessor.Dirty;
			return accessor;
		}

		public DataReaderTransform Set<T>(string name, Func<ITransformedDataRecord, T> getValue) {
			var n = fields.FindIndex(x => x.Name == name);
			return n == -1
			? Add(name, getValue)
			: Transform(name, getValue);
		}

		public DataReaderTransform Rename(string from, string to) {
			var o = GetOrdinal(from);
			fields[o].Name = to;
			ResetRecordFields();
			return this;
		}

		void ResetRecordFields() {
			foreach (var x in fields.OfType<IRecordTransform>())
				x.Reset();
		}

		public override T GetFieldValue<T>(int i) => fields[i].GetFieldValue<T>(this);
		public override object GetValue(int i) => fields[i].GetValue(this);
		public override object GetProviderSpecificValue(int i) => fields[i].GetProviderSpecificValue(this);

		public override object this[int i] => GetValue(i);
		public override object this[string name] => GetValue(GetOrdinal(name));

		public override int Depth => inner.Depth;
		public override bool IsClosed => inner.IsClosed;
		public override int RecordsAffected => inner.RecordsAffected;
		public override int FieldCount => fields.Count;

		public override bool HasRows => throw new NotSupportedException();

		public override void Close() => inner.Close();
		protected override void Dispose(bool disposing) {
			if(disposing)
				inner.Dispose();
		}

		public override Type GetFieldType(int i) => fields[i].FieldType;
		public override Type GetProviderSpecificFieldType(int i) => fields[i].ProviderSpecificFieldType;

		public override bool GetBoolean(int i) => GetFieldValue<bool>(i);
		public override byte GetByte(int i) => GetFieldValue<byte>(i);
		public override char GetChar(int i) => GetFieldValue<char>(i);
		public override DateTime GetDateTime(int i) => GetFieldValue<DateTime>(i);
		public override decimal GetDecimal(int i) => GetFieldValue<decimal>(i);
		public override double GetDouble(int i) => GetFieldValue<double>(i);
		public override float GetFloat(int i) => GetFieldValue<float>(i);
		public override Guid GetGuid(int i) => GetFieldValue<Guid>(i);
		public override short GetInt16(int i) => GetFieldValue<short>(i);
		public override int GetInt32(int i) => GetFieldValue<int>(i);
		public override long GetInt64(int i) => GetFieldValue<long>(i);
		public override string GetString(int i) => GetFieldValue<string>(i);

		public override int GetValues(object[] values) {
			var n = Math.Min(FieldCount, values.Length);
			for (var i = 0; i != n; ++i)
				values[i] = GetValue(i);
			return n;
		}

		public override string GetName(int i) => fields[i].Name;
		public override int GetOrdinal(string name) => fields.FindIndex(x => x.Name == name);

		public override DataTable GetSchemaTable() {
			var schema = new DataReaderSchemaTable();
			for (var i = 0; i != FieldCount; ++i) {
				var item = fields[i];
				schema.Add(item.Name, i, item.FieldType, item.AllowDBNull, item.ColumnSize, 
					providerSpecificDataType: item.ProviderSpecificFieldType);
			}
			return schema.ToDataTable();
		}

		public override string GetDataTypeName(int i) => DataBossDbType.From(GetFieldType(i)).TypeName;

		public override bool IsDBNull(int i) => fields[i].IsDBNull(this);
		public override bool NextResult() => false;
		public override bool Read() {
			onRead?.Invoke();
			return inner.Read();
		}

		public override long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length) => throw new NotSupportedException();
		public override long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length) => throw new NotSupportedException();

		public override IEnumerator GetEnumerator() => new DataReaderEnumerator(this);
	}

	public static class DataReaderTransformExtensions
	{
		public static DbDataReader WithTransform(this IDataReader self, Action<DataReaderTransform> defineTransfrom) {
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
