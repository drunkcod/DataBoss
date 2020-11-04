namespace DataBoss.Data
{
	using System;
	using System.Collections;
	using System.Data;
	using System.Data.Common;
	using System.Diagnostics;
	using DataBoss.Data.Scripting;

	public class ProfiledDataReader : DbDataReader
	{
		readonly IDataReader inner;
		readonly DataBossScripter scripter;
		readonly Stopwatch stopwatch = Stopwatch.StartNew();
		int rowCount = 0;

		public ProfiledDataReader(IDataReader inner, DataBossScripter scripter) {
			this.inner = inner;
			this.scripter = scripter;
		}

		public event EventHandler RowRead;
		public event EventHandler<ProfiledDataReaderClosedEventArgs> Closed;

		public DataBossTableColumn[] GetColumns() => scripter.GetColumns(this);
		
		public override void Close() {
			if(!stopwatch.IsRunning)
				return;

			stopwatch.Stop();
			Closed?.Invoke(this, new ProfiledDataReaderClosedEventArgs(rowCount, stopwatch.Elapsed));
			inner.Close();
		}

		protected override void Dispose(bool disposing) {
			base.Dispose(disposing);
			if (disposing)
				inner.Dispose();
		}

		public override object this[int ordinal] => inner[ordinal];
		public override object this[string name] => inner[name];
		public override int Depth => inner.Depth;
		public override int FieldCount => inner.FieldCount;
		
		public override bool HasRows => inner is DbDataReader x 
			? x.HasRows 
			: throw new NotSupportedException();

		public override bool IsClosed => inner.IsClosed;
		public override int RecordsAffected => inner.RecordsAffected;

		public override bool GetBoolean(int ordinal) => inner.GetBoolean(ordinal);
		public override byte GetByte(int ordinal) => inner.GetByte(ordinal);

		public override long GetBytes(int ordinal, long dataOffset, byte[] buffer, int bufferOffset, int length) =>
			inner.GetBytes(ordinal, dataOffset, buffer, bufferOffset, length);

		public override char GetChar(int ordinal) => inner.GetChar(ordinal);

		public override long GetChars(int ordinal, long dataOffset, char[] buffer, int bufferOffset, int length) =>
			inner.GetChars(ordinal, dataOffset, buffer, bufferOffset, length);

		public override string GetDataTypeName(int ordinal) => inner.GetDataTypeName(ordinal);
		public override DateTime GetDateTime(int ordinal) => inner.GetDateTime(ordinal);
		public override decimal GetDecimal(int ordinal) => inner.GetDecimal(ordinal);
		public override double GetDouble(int ordinal) => inner.GetDouble(ordinal);
		
		public override IEnumerator GetEnumerator() => inner is IEnumerable x
			? x.GetEnumerator()
			: throw new NotSupportedException();
		
		public override Type GetFieldType(int ordinal) => inner.GetFieldType(ordinal);
		public override float GetFloat(int ordinal) => inner.GetFloat(ordinal);
		public override Guid GetGuid(int ordinal) => inner.GetGuid(ordinal);
		public override short GetInt16(int ordinal) => inner.GetInt16(ordinal);
		public override int GetInt32(int ordinal) => inner.GetInt32(ordinal);
		public override long GetInt64(int ordinal) => inner.GetInt64(ordinal);
		public override string GetName(int ordinal) => inner.GetName(ordinal);
		public override int GetOrdinal(string name) => inner.GetOrdinal(name);
		public override string GetString(int ordinal) => inner.GetString(ordinal);
		public override object GetValue(int ordinal) => inner.GetValue(ordinal);
		public override int GetValues(object[] values) => inner.GetValues(values);
		public override bool IsDBNull(int ordinal) => inner.IsDBNull(ordinal);
		public override bool NextResult() => inner.NextResult();
		public override bool Read() {
			if(inner.Read()) {
				++rowCount;
				RowRead?.Invoke(this, EventArgs.Empty);
				return true;
			}
			return false;
		}

		public override DataTable GetSchemaTable() => inner.GetSchemaTable();
	}

	public class ProfiledDataReaderClosedEventArgs : EventArgs
	{
		public readonly TimeSpan Elapsed;
		public readonly int RowCount;

		public ProfiledDataReaderClosedEventArgs(int rowCount, TimeSpan elapsed) {
			this.RowCount = rowCount;
			this.Elapsed = elapsed;
		}
	}
}
