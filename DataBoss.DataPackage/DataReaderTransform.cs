using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using DataBoss.Data;

namespace DataBoss.DataPackage
{
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

		public DataReaderTransform Transform<TOut>(string name, Func<IDataRecord, TOut> transform) {
			transforms[inner.GetOrdinal(name)] = new ConvertToObject<IDataRecord, TOut>(transform).Invoke;
			return this;
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
				var field = fields[i];
				var fieldType = GetFieldType(i);
				var dbType = DataBossDbType.ToDataBossDbType(fieldType);
				schema.Add(GetName(i), i, fieldType, dbType.IsNullable, dbType.ColumnSize);
			}
			return schema.ToDataTable();
		}

		public string GetDataTypeName(int i) => DataBossDbType.ToDataBossDbType(GetFieldType(i)).TypeName;

		public bool IsDBNull(int i) => GetValue(i) is DBNull;
		public bool NextResult() => inner.NextResult();
		public bool Read() => inner.Read();

		public long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length) => throw new NotSupportedException();
		public long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length) => throw new NotSupportedException();
		public IDataReader GetData(int i) => throw new NotSupportedException();

	}
}
