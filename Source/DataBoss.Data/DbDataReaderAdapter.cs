using System;
using System.Collections;
using System.Data;
using System.Data.Common;
using System.Linq.Expressions;
using System.Reflection;

namespace DataBoss.Data
{
	public class DbDataReaderAdapter : DbDataReader
	{
		readonly IDataReader inner;
		readonly IFieldValueReader fieldValueReader;

		interface IFieldValueReader
		{
			T GetFieldValue<T>(int ordinal);
			Type GetProviderSpecificFieldType(int ordinal);
			object GetProviderSpecificValue(int ordinal);
			int GetProviderSpecificValues(object[] values);
		}

		class FieldValueReader<TReader> : IFieldValueReader
		{
			static class FieldValueOfT<T>
			{
				public static readonly Func<TReader, int, T> GetT = FieldValueReader.MakeFieldReader<TReader, T>();
			}

			static readonly Func<TReader, int, Type> getProviderSpecificFieldType;
			static readonly Func<TReader, int, object> getProviderSpecificValue;
			static readonly Func<TReader, object[], int> getProviderSpecificValues;

			readonly TReader instance;

			static FieldValueReader() {
				SetTargetDelegate(ref getProviderSpecificFieldType, nameof(GetProviderSpecificFieldType), () => typeof(IDataRecord).GetMethod(nameof(IDataRecord.GetFieldType)));
				SetTargetDelegate(ref getProviderSpecificValue, nameof(GetProviderSpecificValue), () => typeof(IDataRecord).GetMethod(nameof(IDataRecord.GetValue)));
				SetTargetDelegate(ref getProviderSpecificValues, nameof(GetProviderSpecificValues), () => typeof(IDataRecord).GetMethod(nameof(IDataRecord.GetValues)));
			}

			public FieldValueReader(TReader reader) { 
				this.instance = reader;
			}

			public T GetFieldValue<T>(int ordinal) => FieldValueOfT<T>.GetT(instance, ordinal);
			public Type GetProviderSpecificFieldType(int ordinal) => getProviderSpecificFieldType(instance, ordinal);
			public object GetProviderSpecificValue(int ordinal) => getProviderSpecificValue(instance, ordinal);
			public int GetProviderSpecificValues(object[] values) => getProviderSpecificValues(instance, values);

			static void SetTargetDelegate<T>(ref T target, string optional, Func<MethodInfo> getFallback) where T : Delegate {
				var method = typeof(TReader).GetMethod(optional) ?? getFallback();
				target = Lambdas.CreateDelegate<T>(null, method);
			}
		}

		class FieldValueReader : IFieldValueReader
		{
			readonly IDataReader reader;

			public FieldValueReader(IDataReader reader) { this.reader = reader; }

			public T GetFieldValue<T>(int ordinal) => reader.GetFieldValue<T>(ordinal);
			public Type GetProviderSpecificFieldType(int ordinal) => reader.GetFieldType(ordinal);
			public object GetProviderSpecificValue(int ordinal) => reader.GetValue(ordinal);
			public int GetProviderSpecificValues(object[] values) => reader.GetValues(values);

			public static IFieldValueReader For(IDataReader reader) {
				var readerType = reader.GetType();
				if(readerType.GetMethod(nameof(GetFieldValue)) != null)
					return (IFieldValueReader)Activator.CreateInstance(
						typeof(FieldValueReader<>).MakeGenericType(readerType), 
						reader);
				return new FieldValueReader(reader);
			}

			internal static Func<TReader, int, T> MakeFieldReader<TReader, T>() {
				var x = Expression.Parameter(typeof(TReader), "x");
				var ordinal = Expression.Parameter(typeof(int), "ordinal");
				var getT = x.Type.GetMethod(nameof(GetFieldValue)).MakeGenericMethod(typeof(T));
				return Expression.Lambda<Func<TReader, int, T>>(Expression.Call(x, getT, ordinal), x, ordinal)
					.Compile();
			}
		}

		public DbDataReaderAdapter(IDataReader inner) { 
			this.inner = inner;
			this.fieldValueReader = FieldValueReader.For(inner);
		}

		public override object this[int ordinal] => inner[ordinal];
		public override object this[string name] => inner[name];

		public override bool HasRows => throw new NotSupportedException();

		public override int Depth => inner.Depth;
		public override int FieldCount => inner.FieldCount;
		public override bool IsClosed => inner.IsClosed;
		public override int RecordsAffected => inner.RecordsAffected;

		public override bool GetBoolean(int ordinal) => inner.GetBoolean(ordinal);
		public override byte GetByte(int ordinal) => inner.GetByte(ordinal);

		public override long GetBytes(int ordinal, long dataOffset, byte[] buffer, int bufferOffset, int length) =>
			inner.GetBytes(ordinal, dataOffset, buffer, bufferOffset, length);

		public override char GetChar(int ordinal) => inner.GetChar(ordinal);
		public override long GetChars(int ordinal, long dataOffset, char[] buffer, int bufferOffset, int length) =>
			inner.GetChars(ordinal, dataOffset, buffer, bufferOffset, length);

		public override DateTime GetDateTime(int ordinal) => inner.GetDateTime(ordinal);
		public override decimal GetDecimal(int ordinal) => inner.GetDecimal(ordinal);
		public override double GetDouble(int ordinal) => inner.GetDouble(ordinal);
		public override float GetFloat(int ordinal) => inner.GetFloat(ordinal);
		public override Guid GetGuid(int ordinal) => inner.GetGuid(ordinal);
		public override short GetInt16(int ordinal) => inner.GetInt16(ordinal);
		public override int GetInt32(int ordinal) => inner.GetInt32(ordinal);
		public override long GetInt64(int ordinal) => inner.GetInt64(ordinal);
		public override string GetString(int ordinal) => inner.GetString(ordinal);
		public override object GetValue(int ordinal) => inner.GetValue(ordinal);
		public override int GetValues(object[] values) => inner.GetValues(values);

		public override DataTable GetSchemaTable() => inner.GetSchemaTable();
		public override string GetDataTypeName(int ordinal) => inner.GetDataTypeName(ordinal);
		public override Type GetFieldType(int ordinal) => inner.GetFieldType(ordinal);
		public override string GetName(int ordinal) => inner.GetName(ordinal);
		public override int GetOrdinal(string name) => inner.GetOrdinal(name);

		public override object GetProviderSpecificValue(int ordinal) => fieldValueReader.GetProviderSpecificValue(ordinal);
		public override int GetProviderSpecificValues(object[] values) => fieldValueReader.GetProviderSpecificValues(values);
		public override T GetFieldValue<T>(int ordinal) => fieldValueReader.GetFieldValue<T>(ordinal);
		public override Type GetProviderSpecificFieldType(int ordinal) => fieldValueReader.GetProviderSpecificFieldType(ordinal);

		public override bool IsDBNull(int ordinal) => inner.IsDBNull(ordinal);

		public override void Close() => inner.Close();
		public override bool Read() => inner.Read();
		public override bool NextResult() => inner.NextResult();

		public override IEnumerator GetEnumerator() => new DataReaderEnumerator(inner);
	}
}
