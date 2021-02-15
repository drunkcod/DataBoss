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
		static readonly ConcurrentDictionary<Type, Func<string, int, IFieldAccessor>> fieldAccessorCtorCache = new();

		static IFieldAccessor CreateFieldAccessor(string name, int ordinal, Type type) {
			var ctor = fieldAccessorCtorCache.GetOrAdd(type, fieldType => {
				var p0 = Expression.Parameter(typeof(string));
				var p1 = Expression.Parameter(typeof(int));
				var c = typeof(FieldAccessor<>).MakeGenericType(fieldType).GetConstructor(new[] { typeof(string), typeof(int) });
				return Expression.Lambda<Func<string, int, IFieldAccessor>>(Expression.New(c, p0, p1), p0, p1)
					.Compile();
			});

			return ctor(name, ordinal);
		}

		readonly IDataReader inner;

		interface IFieldAccessor
		{
			string Name { get; }
			Type FieldType { get; }

			bool IsDBNull(IDataRecord record);
			object GetValue(IDataRecord record);
			T GetFieldValue<T>(IDataRecord record);
		}

		class FieldAccessor<TField> : IFieldAccessor
		{
			readonly int ordinal;
			public string Name { get; }

			public Type FieldType => typeof(TField);

			public FieldAccessor(string name, int ordinal) {
				this.Name = name;
				this.ordinal = ordinal;
			}

			public bool IsDBNull(IDataRecord record) => record.IsDBNull(ordinal);
			public object GetValue(IDataRecord record) => GetFieldValue<TField>(record);
			public T GetFieldValue<T>(IDataRecord record) => record.GetFieldValue<T>(ordinal);
		}

		class FieldTransform<T> : IFieldAccessor
		{
			readonly Func<IDataRecord, T> transform;
			readonly int ordinal;

			public FieldTransform(string name, int ordinal, Func<IDataRecord, T> transform) {
				this.Name = name;
				this.ordinal = ordinal;
				this.transform = transform;
			}

			public string Name { get; }
			public Type FieldType => typeof(T);

			public bool IsDBNull(IDataRecord record) => (ordinal != -1) && record.IsDBNull(ordinal);
			public object GetValue(IDataRecord record) => Invoke(record);
			public T1 GetFieldValue<T1>(IDataRecord record) => (T1)(object)Invoke(record);

			T Invoke(IDataRecord record) => transform(record);
		}

		class FieldTransform<TField, T> : IFieldAccessor
		{
			readonly Func<TField, T> transform;
			readonly int ordinal;

			public FieldTransform(string name, int ordinal, Func<TField, T> transform) {
				this.Name = name;
				this.ordinal = ordinal;
				this.transform = transform;
			}

			public string Name { get; }
			public Type FieldType => typeof(T);

			public bool IsDBNull(IDataRecord record) => record.IsDBNull(ordinal);
			public object GetValue(IDataRecord record) => Invoke(record);

			public T1 GetFieldValue<T1>(IDataRecord record) =>
				(T1)(object)Invoke(record);

			T Invoke(IDataRecord record) => transform(record.GetFieldValue<TField>(ordinal));
		}

		readonly List<IFieldAccessor> fields = new();

		public DataReaderTransform(IDataReader inner) {
			this.inner = inner;
			this.fields.AddRange(Enumerable.Range(0, inner.FieldCount).Select(n => CreateFieldAccessor(inner.GetName(n), n, inner.GetFieldType(n))));
		}

		public DataReaderTransform Add<T>(string name, Func<IDataRecord, T> getValue) {
			fields.Add(new FieldTransform<T>(name, -1, getValue));
			return this;
		}

		public DataReaderTransform Remove(string name) {
			fields.RemoveAll(x => x.Name == name);
			return this;
		}

		public DataReaderTransform Transform<T>(string name, Func<IDataRecord, T> transform) {
			var o = inner.GetOrdinal(name);
			fields[o] = new FieldTransform<T>(name, o, transform);
			return this;
		}

		public DataReaderTransform Transform<TField, T>(string name, Func<TField, T> transform) {
			var o = inner.GetOrdinal(name);
			fields[o] = new FieldTransform<TField, T>(name, o, transform);
			return this;
		}

		public DataReaderTransform Transform<TField, T>(int ordinal, Func<TField, T> transform) {
			fields[ordinal] = new FieldTransform<TField, T>(inner.GetName(ordinal), ordinal, transform);
			return this;
		}

		public DataReaderTransform Set<T>(string name, Func<IDataRecord, T> getValue) {
			var n = fields.FindIndex(x => x.Name == name);
			return n == -1
			? Add(name, getValue)
			: Transform(name, getValue);
		}

		public T GetFieldValue<T>(int i) => fields[i].GetFieldValue<T>(inner);
		public object GetValue(int i) => fields[i].GetValue(inner);

		object IDataRecord.this[int i] => GetValue(i);
		object IDataRecord.this[string name] => GetValue(GetOrdinal(name));
		object IDataRecord.GetValue(int i) => GetValue(i);

		int IDataReader.Depth => inner.Depth;
		bool IDataReader.IsClosed => inner.IsClosed;
		int IDataReader.RecordsAffected => inner.RecordsAffected;
		public int FieldCount => fields.Count;

		void IDataReader.Close() => inner.Close();
		void IDisposable.Dispose() => inner.Dispose();

		public Type GetFieldType(int i) => fields[i].FieldType;
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
				var fieldType = GetFieldType(i);
				var dbType = DataBossDbType.From(fieldType);
				schema.Add(GetName(i), i, fieldType, dbType.IsNullable, dbType.ColumnSize);
			}
			return schema.ToDataTable();
		}

		string IDataRecord.GetDataTypeName(int i) => DataBossDbType.From(GetFieldType(i)).TypeName;

		public bool IsDBNull(int i) => fields[i].IsDBNull(inner);
		bool IDataReader.NextResult() => inner.NextResult();
		bool IDataReader.Read() => inner.Read();

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
