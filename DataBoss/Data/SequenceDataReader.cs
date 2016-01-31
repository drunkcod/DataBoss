using System;
using System.Collections.Generic;
using System.Data;
using System.Linq.Expressions;

namespace DataBoss.Data
{
	public static class SequenceDataReader
	{
		public static SequenceDataReader<T> For<T>(IEnumerable<T> data) {
			return new SequenceDataReader<T>(data.GetEnumerator());
		}
	}

	public class SequenceDataReader<T> : IDataReader
	{
		readonly IEnumerator<T> data;
		Func<T, object>[] selectors = new Func<T,object>[0];
		object[] current = new object[0];
		string[] fieldNames = new string[0];
	
		public SequenceDataReader(IEnumerator<T> data) {
			this.data = data;
		}

		public int Map<TMember>(Expression<Func<T,TMember>> selector) {
			if(selector.Body.NodeType != ExpressionType.MemberAccess)
				throw new NotSupportedException();
			var m = (MemberExpression)selector.Body;
			return Map(m.Member.Name, Expression.Lambda<Func<T,object>>(Expression.Convert(m, typeof(object)), true, selector.Parameters).Compile());
		}

		public int Map(string name, Func<T, object> selector) {
			var ordinal = FieldCount;
			Array.Resize(ref selectors, ordinal + 1);
			Array.Resize(ref fieldNames, ordinal + 1);
			Array.Resize(ref current, FieldCount);

			fieldNames[ordinal] = name;
			selectors[ordinal] = selector;

			return ordinal;
		}

		object IDataRecord.this[int i] => GetValue(i);
		object IDataRecord.this[string name] => GetValue(GetOrdinal(name));

		public int FieldCount => selectors.Length;
	
		public bool Read() { 
			if(!data.MoveNext())
				return false;
			var source = data.Current;
			for(var i = 0; i != current.Length; ++i)
				current[i] = selectors[i](source);
			return true;
		}

		public bool NextResult() { return false; }

		public void Close() { }

		public void Dispose() { data.Dispose(); }

		public int GetOrdinal(string name) {
			for(var i = 0; i != FieldCount; ++i)
				if(fieldNames[i] == name)
					return i;
			throw new InvalidOperationException($"No field named '{name}' mapped");
		}

		public object GetValue(int i) { return current[i]; }
		//SqlBulkCopy.EnableStreaming requires this
		public bool IsDBNull(int i) { return GetValue(i) is DBNull; }

	#region Here Be Dragons (not implemented / supported)
		public int Depth { get { throw new NotSupportedException(); } }
		public bool IsClosed { get { throw new NotSupportedException(); } }
		public int RecordsAffected { get { throw new NotSupportedException(); } }
		public string GetName(int i) { throw new NotImplementedException(); }
		public string GetDataTypeName(int i) { throw new NotImplementedException(); }
		public Type GetFieldType(int i) { throw new NotImplementedException(); }
		public int GetValues(object[] values) { throw new NotImplementedException(); }
		public bool GetBoolean(int i) { throw new NotImplementedException(); }
		public byte GetByte(int i) { throw new NotImplementedException(); }
		public long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length) { throw new NotImplementedException(); }
		public char GetChar(int i) { throw new NotImplementedException(); }
		public long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length) { throw new NotImplementedException(); }
		public Guid GetGuid(int i) { throw new NotImplementedException(); }
		public short GetInt16(int i) { throw new NotImplementedException(); }
		public int GetInt32(int i) { throw new NotImplementedException(); }
		public long GetInt64(int i) { throw new NotImplementedException(); }
		public float GetFloat(int i) { throw new NotImplementedException(); }
		public double GetDouble(int i) { throw new NotImplementedException(); }
		public string GetString(int i) { throw new NotImplementedException(); }
		public decimal GetDecimal(int i) { throw new NotImplementedException(); }
		public DateTime GetDateTime(int i) { throw new NotImplementedException(); }
		public IDataReader GetData(int i) { throw new NotImplementedException(); }
		public DataTable GetSchemaTable() { throw new NotImplementedException(); }
	#endregion
	}
}
