using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using DataBoss.Data;

namespace DataBoss.DataPackage
{
	class WhereDataReader : IDataReader
	{
		readonly IDataReader inner;
		readonly Func<IDataRecord, bool> predicate;

		public WhereDataReader(IDataReader inner, Func<IDataRecord, bool> predicate) {
			this.inner = inner;
			this.predicate = predicate;
		}

		public bool Read() {
			while(inner.Read())
				if(predicate(inner))
					return true;
			return false;
		}

		public object this[int i] => inner[i];
		public object this[string name] => inner[name];

		public int Depth => inner.Depth;
		public bool IsClosed => inner.IsClosed;

		public int RecordsAffected => inner.RecordsAffected;
		public int FieldCount => inner.FieldCount;

		public void Close() => inner.Close();
		public void Dispose() => inner.Dispose();

		public bool GetBoolean(int i) => inner.GetBoolean(i);
		public byte GetByte(int i) => inner.GetByte(i);
		public long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length) => inner.GetBytes(i, fieldOffset, buffer, bufferoffset, length);
		public char GetChar(int i) => inner.GetChar(i);
		public long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length) => inner.GetChars(i, fieldoffset, buffer, bufferoffset, length);
		public IDataReader GetData(int i) => inner.GetData(i);
		public string GetDataTypeName(int i) => inner.GetDataTypeName(i);
		public DateTime GetDateTime(int i) => inner.GetDateTime(i);
		public decimal GetDecimal(int i) => inner.GetDecimal(i);
		public double GetDouble(int i) => inner.GetDouble(i);
		public Type GetFieldType(int i) => inner.GetFieldType(i);
		public float GetFloat(int i) => inner.GetFloat(i);
		public Guid GetGuid(int i) => inner.GetGuid(i);
		public short GetInt16(int i) => inner.GetInt16(i);
		public int GetInt32(int i) => inner.GetInt32(i);
		public long GetInt64(int i) => inner.GetInt64(i);
		public string GetName(int i) => inner.GetName(i);
		public int GetOrdinal(string name) => inner.GetOrdinal(name);
		public DataTable GetSchemaTable() => inner.GetSchemaTable();
		public string GetString(int i) => inner.GetString(i);
		public object GetValue(int i) => inner.GetValue(i);
		public int GetValues(object[] values) => inner.GetValues(values);
		public bool IsDBNull(int i) => inner.IsDBNull(i);

		public bool NextResult() => inner.NextResult();
	}

	public class DataReaderTransform : IDataReader
	{
		readonly IDataReader inner;

		interface IConvertToObject
		{
			Type ToType { get; }
		}

		class ConvertToObject<TIn, TOut> : IConvertToObject
		{
			readonly Func<TIn, TOut> inner;

			public ConvertToObject(Func<TIn, TOut> inner) { this.inner = inner; }

			public Type ToType => typeof(TOut);

			public object Invoke(TIn value) => inner(value);
		}

		readonly List<(string Name, int Ordinal, Func<IDataRecord, object> GetValue)> fields = new List<(string, int, Func<IDataRecord, object>)>();
		Func<IDataRecord, object>[] transforms;

		public DataReaderTransform(IDataReader inner) {
			this.inner = inner;
			this.fields.AddRange(Enumerable.Range(0, inner.FieldCount).Select(n => (inner.GetName(n), n, (Func<IDataRecord, object>)null)));
			this.transforms = new Func<IDataRecord, object>[inner.FieldCount];
		}

		public DataReaderTransform Add<T>(string name, Func<IDataRecord, T> getValue) {
			fields.Add((name, -1, new ConvertToObject<IDataRecord, T>(getValue).Invoke));
			return this;
		}

		public DataReaderTransform Remove(string name) {
			fields.RemoveAll(x => x.Name == name);
			return this;
		}

		public DataReaderTransform Transform<T>(string name, Func<IDataRecord, T> transform) {
			transforms[inner.GetOrdinal(name)] = new ConvertToObject<IDataRecord, T>(transform).Invoke;
			return this;
		}

		public DataReaderTransform Set<T>(string name, Func<IDataRecord, T> getValue) {
			var n = fields.FindIndex(x => x.Name == name);
			return n == -1
				? Add(name, getValue)
				: Transform(name, getValue);
		}

		public object GetValue(int i) {
			var field = fields[i];
			if (field.Ordinal != -1) {
				var value = inner.GetValue(field.Ordinal);
				return transforms[field.Ordinal]?.Invoke(inner) ?? value;
			}
			return field.GetValue(this);
		}

		public object this[int i] => GetValue(i);
		public object this[string name] => this[GetOrdinal(name)];

		public int Depth => inner.Depth;
		public bool IsClosed => inner.IsClosed;
		public int RecordsAffected => inner.RecordsAffected;
		public int FieldCount => fields.Count;

		public void Close() => inner.Close();
		public void Dispose() => inner.Dispose();

		public Type GetFieldType(int i) {
			var field = fields[i];
			if (field.Ordinal == -1)
				return ((IConvertToObject)field.GetValue.Target).ToType;
			var transform = transforms[field.Ordinal];
			if (transform == null)
				return inner.GetFieldType(field.Ordinal);
			return ((IConvertToObject)transform.Target).ToType;
		}

		public bool GetBoolean(int i) => (bool)GetValue(i);
		public byte GetByte(int i) => (byte)GetValue(i);
		public char GetChar(int i) => (char)GetValue(i);
		public DateTime GetDateTime(int i) => (DateTime)GetValue(i);
		public decimal GetDecimal(int i) => (decimal)GetValue(i);
		public double GetDouble(int i) => (double)GetValue(i);
		public float GetFloat(int i) => (float)GetValue(i);
		public Guid GetGuid(int i) => (Guid)GetValue(i);
		public short GetInt16(int i) => (short)GetValue(i);
		public int GetInt32(int i) => (int)GetValue(i);
		public long GetInt64(int i) => (long)GetValue(i);
		public string GetString(int i) => (string)GetValue(i);

		public int GetValues(object[] values) {
			var n = Math.Min(FieldCount, values.Length);
			for (var i = 0; i != n; ++i)
				values[i] = GetValue(i);
			return n;
		}

		public string GetName(int i) => fields[i].Name;
		public int GetOrdinal(string name) => fields.FindIndex(x => x.Name == name);

		public DataTable GetSchemaTable() {
			var schema = new DataReaderSchemaTable();
			for (var i = 0; i != FieldCount; ++i) {
				var fieldType = GetFieldType(i);
				var dbType = DataBossDbType.From(fieldType);
				schema.Add(GetName(i), i, fieldType, dbType.IsNullable, dbType.ColumnSize);
			}
			return schema.ToDataTable();
		}

		public string GetDataTypeName(int i) => DataBossDbType.From(GetFieldType(i)).TypeName;

		public bool IsDBNull(int i) => GetValue(i) is DBNull;
		public bool NextResult() => inner.NextResult();
		public bool Read() => inner.Read();

		public long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length) => throw new NotSupportedException();
		public long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length) => throw new NotSupportedException();
		public IDataReader GetData(int i) => throw new NotSupportedException();

	}
}
