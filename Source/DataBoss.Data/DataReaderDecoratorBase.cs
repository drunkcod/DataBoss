using System;
using System.Collections;
using System.Data;
using System.Data.Common;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace DataBoss.Data
{
	public class DataReaderDecoratorBase : DbDataReader
	{
		DbDataReader inner;

		protected DbDataReader Inner
		{
			get => inner;
			set => inner = value ?? throw new ArgumentNullException(nameof(Inner));
		}

		public DataReaderDecoratorBase(IDataReader inner) : 
			this(inner is DbDataReader dbDataReader ? dbDataReader : new DbDataReaderAdapter(inner))
		{ }

		public DataReaderDecoratorBase(DbDataReader inner) {
			this.Inner = inner;
		}

		public override object this[int ordinal] => Inner[ordinal];
		public override object this[string name] => Inner[name];
		public override int Depth => Inner.Depth;
		public override int FieldCount => Inner.FieldCount;
		public override int VisibleFieldCount => Inner.VisibleFieldCount;
		public override bool HasRows => Inner.HasRows;
		public override bool IsClosed => Inner.IsClosed;
		public override int RecordsAffected => Inner.RecordsAffected;

		public override void Close() => Inner.Close();

		public override bool Read() => Inner.Read();
		public override Task<bool> ReadAsync(CancellationToken cancellationToken) => Inner.ReadAsync(cancellationToken);

		public override bool NextResult() => Inner.NextResult();
		public override Task<bool> NextResultAsync(CancellationToken cancellationToken) => Inner.NextResultAsync(cancellationToken);

		protected override void Dispose(bool disposing) {
			if(disposing)
				Inner.Dispose();
		}

#if NETSTANDARD2_1_OR_GREATER
		public override Task CloseAsync() => Inner.CloseAsync();
		public override ValueTask DisposeAsync() => Inner.DisposeAsync();

		public override Task<T> GetFieldValueAsync<T>(int ordinal, CancellationToken cancellationToken) => 
			Inner.GetFieldValueAsync<T>(ordinal, cancellationToken);
#endif

		public override DataTable GetSchemaTable() => Inner.GetSchemaTable();
		public override Type GetProviderSpecificFieldType(int ordinal) => Inner.GetProviderSpecificFieldType(ordinal);
		public override object GetProviderSpecificValue(int ordinal) => Inner.GetProviderSpecificValue(ordinal);
		public override int GetProviderSpecificValues(object[] values) => Inner.GetProviderSpecificValues(values);
		public override Stream GetStream(int ordinal) => Inner.GetStream(ordinal);
		public override TextReader GetTextReader(int ordinal) => Inner.GetTextReader(ordinal);

		public override bool GetBoolean(int ordinal) => Inner.GetBoolean(ordinal);
		public override byte GetByte(int ordinal) => Inner.GetByte(ordinal);

		public override long GetBytes(int ordinal, long dataOffset, byte[] buffer, int bufferOffset, int length) =>
			Inner.GetBytes(ordinal, dataOffset, buffer, bufferOffset, length);

		public override char GetChar(int ordinal) => Inner.GetChar(ordinal);

		public override long GetChars(int ordinal, long dataOffset, char[] buffer, int bufferOffset, int length) =>
			Inner.GetChars(ordinal, dataOffset, buffer, bufferOffset, length);

		public override string GetDataTypeName(int ordinal) => Inner.GetDataTypeName(ordinal);
		public override DateTime GetDateTime(int ordinal) => Inner.GetDateTime(ordinal);
		public override decimal GetDecimal(int ordinal) => Inner.GetDecimal(ordinal);
		public override double GetDouble(int ordinal) => Inner.GetDouble(ordinal);
		public override IEnumerator GetEnumerator() => Inner.GetEnumerator();
		public override Type GetFieldType(int ordinal) => Inner.GetFieldType(ordinal);
		public override float GetFloat(int ordinal) => Inner.GetFloat(ordinal);
		public override Guid GetGuid(int ordinal) => Inner.GetGuid(ordinal);
		public override short GetInt16(int ordinal) => Inner.GetInt16(ordinal);
		public override int GetInt32(int ordinal) => Inner.GetInt32(ordinal);
		public override long GetInt64(int ordinal) => Inner.GetInt64(ordinal);
		public override string GetName(int ordinal) => Inner.GetName(ordinal);
		public override int GetOrdinal(string name) => Inner.GetOrdinal(name);
		public override string GetString(int ordinal) => Inner.GetString(ordinal);
		public override object GetValue(int ordinal) => Inner.GetValue(ordinal);
		public override int GetValues(object[] values) => Inner.GetValues(values);

		public override T GetFieldValue<T>(int ordinal) => Inner.GetFieldValue<T>(ordinal);

		public override bool IsDBNull(int ordinal) => Inner.IsDBNull(ordinal);
		public override Task<bool> IsDBNullAsync(int ordinal, CancellationToken cancellationToken) => Inner.IsDBNullAsync(ordinal, cancellationToken);
	}
}
