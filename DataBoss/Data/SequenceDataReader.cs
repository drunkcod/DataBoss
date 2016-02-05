using System;
using System.Collections.Generic;
using System.Data;
using System.Linq.Expressions;
using System.Reflection;

namespace DataBoss.Data
{
	public static class SequenceDataReader
	{
		public static SequenceDataReader<T> For<T>(IEnumerable<T> data) {
			return new SequenceDataReader<T>(data.GetEnumerator());
		}

		public static SequenceDataReader<T> Create<T>(IEnumerable<T> data, params string[] members) {
			var reader = For(data);
			Array.ForEach(members, x => reader.Map(x));
			return reader;
		}

		public static SequenceDataReader<T> Create<T>(IEnumerable<T> data, params MemberInfo[] members) {
			var reader = For(data);
			Array.ForEach(members, x => reader.Map(x));
			return reader;
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

		public int Map(string memberName) {
			var memberInfo = (typeof(T).GetField(memberName) as MemberInfo) ?? typeof(T).GetProperty(memberName);
			if(memberInfo == null)
				throw new InvalidOperationException($"Can't find public field or property '{memberName}'");
			return Map(memberInfo);
		}

		public int Map<TMember>(Expression<Func<T,TMember>> selector) {
			if(selector.Body.NodeType != ExpressionType.MemberAccess)
				throw new NotSupportedException();
			var m = (MemberExpression)selector.Body;
			return Map(m.Member);
		}

		public int Map(MemberInfo memberInfo) {
			var arg0 = Expression.Parameter(typeof(T), "x");
			var m = Expression.MakeMemberAccess(arg0, memberInfo);
			return Map(m.Member.Name, Expression.Lambda<Func<T,object>>(Expression.Convert(m, typeof(object)), true, arg0).Compile());
		}

		public int Map(string name, Func<T, object> selector) {
			var ordinal = FieldCount;
			Array.Resize(ref selectors, ordinal + 1);
			Array.Resize(ref fieldNames, ordinal + 1);
			Array.Resize(ref current, ordinal + 1);

			fieldNames[ordinal] = name;
			selectors[ordinal] = selector;

			return ordinal;
		}

		public object this[int i] => GetValue(i);
		public object this[string name] => GetValue(GetOrdinal(name));

		public int FieldCount => selectors.Length;
	
		public bool Read() { 
			if(!data.MoveNext())
				return false;
			var source = data.Current;
			for(var i = 0; i != current.Length; ++i)
				current[i] = selectors[i](source);
			return true;
		}

		public bool NextResult() => false;

		public void Close() { }

		public void Dispose() => data.Dispose();

		public string GetName(int i) { return fieldNames[i]; }
		public int GetOrdinal(string name) {
			for(var i = 0; i != fieldNames.Length; ++i)
				if(fieldNames[i] == name)
					return i;
			throw new InvalidOperationException($"No field named '{name}' mapped");
		}

		public object GetValue(int i) => current[i];
		//SqlBulkCopy.EnableStreaming requires this
		public bool IsDBNull(int i) => GetValue(i) is DBNull;

	#region Here Be Dragons (not implemented / supported)
		int IDataReader.Depth { get { throw new NotSupportedException(); } }
		bool IDataReader.IsClosed { get { throw new NotSupportedException(); } }
		int IDataReader.RecordsAffected { get { throw new NotSupportedException(); } }
		DataTable IDataReader.GetSchemaTable() { throw new NotSupportedException(); }

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
	#endregion
	}
}
