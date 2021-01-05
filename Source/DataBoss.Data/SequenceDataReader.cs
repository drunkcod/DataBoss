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

	public interface IDataRecordReader
	{
		bool Read();
		IDataRecord GetRecord();
	}

	public class SequenceDataReader<T> : IDataReader, IDataRecordReader
	{
		abstract class ColumnAccessor
		{

			public abstract object GetValue(T item);

			public virtual bool GetBoolean(T item) => (bool)GetValue(item);
			public virtual int GetInt32(T item) => (int)GetValue(item);
			public virtual float GetFloat(T item) => (float)GetValue(item);
			public virtual double GetDouble(T item) => (double)GetValue(item);
			public virtual string GetString(T item) => (string)GetValue(item);
		}

		sealed class ObjectColumnAccessor : ColumnAccessor
		{
			readonly Func<T, object> get;

			public ObjectColumnAccessor(Func<T, object> get) {
				this.get = get;
			}

			public override object GetValue(T item) => get(item);
		}

		sealed class Int32ColumnAccessor : ColumnAccessor
		{
			readonly Func<T, int> get;

			public Int32ColumnAccessor(Func<T, int> get) {
				this.get = get;
			}

			public override int GetInt32(T item) => get(item);
			public override object GetValue(T item) => get(item);
		}

		sealed class SingleColumnAccessor : ColumnAccessor
		{
			readonly Func<T, float> get;

			public SingleColumnAccessor(Func<T, float> get) {
				this.get = get;
			}

			public override float GetFloat(T item) => get(item);
			public override object GetValue(T item) => get(item);
		}

		sealed class DoubleColumnAccessor : ColumnAccessor
		{
			readonly Func<T, double> get;

			public DoubleColumnAccessor(Func<T, double> get) {
				this.get = get;
			}

			public override double GetDouble(T item) => get(item);
			public override object GetValue(T item) => get(item);
		}

		sealed class StringColumnAccessor : ColumnAccessor
		{
			readonly Func<T, string> get;

			public StringColumnAccessor(Func<T, string> get) {
				this.get = get;
			}

			public override string GetString(T item) => get(item);
			public override object GetValue(T item) => get(item);
		}

		sealed class BooleanColumnAccessor : ColumnAccessor
		{
			readonly Func<T, bool> get;

			public BooleanColumnAccessor(Func<T, bool> get) {
				this.get = get;
			}

			public override bool GetBoolean(T item) => get(item);
			public override object GetValue(T item) => get(item);
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

			public bool IsDBNull(int i) => parent.IsDBNull(item, i);
			public object GetValue(int i) => GetAccessor(i).GetValue(item);
			public int GetInt32(int i) => GetAccessor(i).GetInt32(item);
			public float GetFloat(int i) => GetAccessor(i).GetFloat(item);
			public double GetDouble(int i) => GetAccessor(i).GetDouble(item);
			public string GetString(int i) => GetAccessor(i).GetString(item);
			public bool GetBoolean(int i) => GetAccessor(i).GetBoolean(item);

			ColumnAccessor GetAccessor(int i) => parent.accessors[i];

			public int GetValues(object[] values) {
				var n = Math.Min(FieldCount, values.Length);
				for (var i = 0; i != n; ++i)
					values[i] = GetValue(i);
				return n;
			}

			public object this[int i] => GetValue(i);
			public object this[string name] => GetValue(GetOrdinal(name));

			public byte GetByte(int i) => (byte)GetValue(i);
			public long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length) => throw new NotImplementedException();
			public char GetChar(int i) => (char)GetValue(i);
			public long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length) => throw new NotImplementedException();
			public Guid GetGuid(int i) => (Guid)GetValue(i);
			public short GetInt16(int i) => (short)GetValue(i);
			public long GetInt64(int i) => (long)GetValue(i);
			public decimal GetDecimal(int i) => (decimal)GetValue(i);
			public DateTime GetDateTime(int i) => (DateTime)GetValue(i);
			public IDataReader GetData(int i) => throw new NotImplementedException();

			public string GetDataTypeName(int i) => parent.GetDataTypeName(i);
			public Type GetFieldType(int i) => parent.GetFieldType(i);
			public string GetName(int i) => parent.GetName(i);
			public int GetOrdinal(string name) => parent.GetOrdinal(name);
		}

		readonly ColumnAccessor[] accessors;
		readonly IEnumerator<T> data;
		readonly DataReaderSchemaTable schema = new DataReaderSchemaTable();
	
		internal SequenceDataReader(IEnumerator<T> data, FieldMapping<T> fields) {
			this.data = data ?? throw new ArgumentNullException(nameof(data));
			var fieldNames = fields.GetFieldNames();
			var fieldTypes = fields.GetFieldTypes();
			var dbTypes = fields.GetDbTypes();
			for(var i = 0; i != fieldNames.Length; ++i) 
				schema.Add(fieldNames[i], i, fieldTypes[i], dbTypes[i].IsNullable, columnSize: dbTypes[i].ColumnSize, dataTypeName: dbTypes[i].TypeName); 
			this.accessors = new ColumnAccessor[fields.Count];
			for (var i = 0; i != accessors.Length; ++i)
				accessors[i] = MakeAccessor(fields.Source, fields.GetSelector(i));
		}

		static ColumnAccessor MakeAccessor(ParameterExpression source, Expression selector) {
			switch(Type.GetTypeCode(selector.Type)) {
				default: return new ObjectColumnAccessor(CompileSelector<object>(source, selector.Box()));
				case TypeCode.Int32: return new Int32ColumnAccessor(CompileSelector<int>(source, selector));
				case TypeCode.Single: return new SingleColumnAccessor(CompileSelector<float>(source, selector));
				case TypeCode.Double: return new DoubleColumnAccessor(CompileSelector<double>(source, selector));
				case TypeCode.String: return new StringColumnAccessor(CompileSelector<string>(source, selector));
				case TypeCode.Boolean: return new BooleanColumnAccessor(CompileSelector<bool>(source, selector));
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
