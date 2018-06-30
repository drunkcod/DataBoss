using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace DataBoss.Data
{
	public class ProfiledSqlConnection : DbConnection
	{
		readonly SqlConnection inner;

		public ProfiledSqlConnection(SqlConnection inner) {
			this.inner = inner;
		}

		public override string ConnectionString { 
			get => inner.ConnectionString; 
			set => inner.ConnectionString = value; 
		}
	
		public override string Database => inner.Database;
		public override string DataSource => inner.DataSource;
		public override string ServerVersion => inner.ServerVersion;
		public override ConnectionState State => inner.State;

		public event EventHandler<ProfiledSqlCommandExecutingEventArgs> CommandExecuting;
		public event EventHandler<ProfiledSqlCommandExecutedEventArgs> CommandExecuted;

		public event EventHandler<ProfiledSqlCommandExecutingEventArgs> ReaderCreated;
		public event EventHandler<ProfiledSqlCommandExecutedEventArgs> ReaderClosed;

		protected override DbCommand CreateDbCommand() => new ProfiledSqlCommand(this, inner.CreateCommand());
		protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel) => inner.BeginTransaction(isolationLevel);
		public override void Close() => inner.Close();
		public override void ChangeDatabase(string databaseName) => inner.ChangeDatabase(databaseName);
		public override void Open() => inner.Open();
		public override Task OpenAsync(CancellationToken cancellationToken) => inner.OpenAsync(cancellationToken);

		internal void Into<T>(string destinationTable, IEnumerable<T> rows) =>
			inner.Into(destinationTable, rows);

		internal T Execute<T>(ProfiledSqlCommand command, ExecuteT<T> executeT) {
			var scope = OnExecuting(command);
			scope.OnExecuted(executeT(command.inner, out var r));
			return r;
		}

		internal class ExecutionScope
		{
			readonly ProfiledSqlCommand parent;
			readonly Stopwatch stopwatch = Stopwatch.StartNew();

			public ExecutionScope(ProfiledSqlCommand parent) {
				this.parent = parent;
			}

			public void OnExecuted(int rowCount) =>
				((ProfiledSqlConnection)parent.Connection).CommandExecuted?.Invoke(this, new ProfiledSqlCommandExecutedEventArgs(parent, stopwatch.Elapsed, rowCount));

			public void OnReaderClosed(int rowCount) =>
				((ProfiledSqlConnection)parent.Connection).ReaderClosed?.Invoke(this, new ProfiledSqlCommandExecutedEventArgs(parent, stopwatch.Elapsed, rowCount));
		}

		internal ExecutionScope OnExecuting(ProfiledSqlCommand command) {
			CommandExecuting?.Invoke(this, new ProfiledSqlCommandExecutingEventArgs(command));
			return new ExecutionScope(command);
		}

		internal ExecutionScope OnReaderCreated(ProfiledSqlCommand command) {
			ReaderCreated?.Invoke(this, new ProfiledSqlCommandExecutingEventArgs(command));
			return new ExecutionScope(command);
		}
	}

	delegate int ExecuteT<T>(SqlCommand command, out T result);

	public class ProfiledSqlCommand : DbCommand
	{

		static readonly ExecuteT<int> DoExecuteNonQuery = (SqlCommand c, out int result) => result = c.ExecuteNonQuery();
		static readonly ExecuteT<object> DoExecuteScalar = (SqlCommand c, out object result) => {
			result = c.ExecuteScalar();
			return 1;
		};

		readonly ProfiledSqlConnection parent;
		readonly internal SqlCommand inner;

		internal ProfiledSqlCommand(ProfiledSqlConnection parent, SqlCommand inner) {
			this.parent = parent;
			this.inner = inner;
		}

		public override string CommandText {
			get => inner.CommandText;
			set => inner.CommandText = value;
		}

		public override int CommandTimeout {
			get => inner.CommandTimeout;
			set => inner.CommandTimeout = value;
		}

		public override CommandType CommandType {
			get => inner.CommandType;
			set => inner.CommandType = value;
		}

		public override bool DesignTimeVisible {
			get => inner.DesignTimeVisible;
			set => inner.DesignTimeVisible = value;
		}

		public override UpdateRowSource UpdatedRowSource {
			get => inner.UpdatedRowSource;
			set => inner.UpdatedRowSource = value;
		}

		public new SqlParameterCollection Parameters => inner.Parameters;

		protected override DbConnection DbConnection {
			get => parent;
			set => throw new NotSupportedException("Can't switch connection for profiled command");
		}

		protected override DbParameterCollection DbParameterCollection => inner.Parameters;

		protected override DbTransaction DbTransaction {
			get => inner.Transaction;
			set => inner.Transaction = (SqlTransaction)value;
		}

		public override void Cancel() => inner.Cancel();

		public override int ExecuteNonQuery() => parent.Execute(this, DoExecuteNonQuery);
		public override object ExecuteScalar() => parent.Execute(this, DoExecuteScalar);
		public new SqlDataReader ExecuteReader(CommandBehavior behavior) => inner.ExecuteReader(behavior);

		protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior) {
			var s = parent.OnExecuting(this);
			var reader = ExecuteReader(behavior);
			var t = parent.OnReaderCreated(this);
			var r = new ProfiledSqlDataReader(reader, t.OnReaderClosed);
			s.OnExecuted(0);
			return r;
		}

		public override void Prepare() => inner.Prepare();

		protected override DbParameter CreateDbParameter() => inner.CreateParameter();
	}

	class ProfiledSqlDataReader : DbDataReader
	{
		readonly SqlDataReader inner;
		int rowCount = 0;
		Action<int> onClose;

		internal ProfiledSqlDataReader(SqlDataReader inner, Action<int> onClose) {
			this.inner = inner;
			this.onClose = onClose;
		}
		
		public override void Close() {
			if(onClose == null)
				return;
			onClose(rowCount);
			onClose = null;
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
		public override bool HasRows => inner.HasRows;
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
		public override IEnumerator GetEnumerator() => inner.GetEnumerator();
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
				return true;
			}
			return false;
		}

		public override DataTable GetSchemaTable() => inner.GetSchemaTable();
	}

	public class ProfiledSqlCommandExecutingEventArgs : EventArgs
	{
		public readonly ProfiledSqlCommand Command;

		public ProfiledSqlCommandExecutingEventArgs(ProfiledSqlCommand command) {
			this.Command = command;
		}
	}

	public class ProfiledSqlCommandExecutedEventArgs : EventArgs
	{
		public readonly ProfiledSqlCommand Command;
		public readonly TimeSpan Elapsed;
		public readonly int RowCount;

		public ProfiledSqlCommandExecutedEventArgs(ProfiledSqlCommand command, TimeSpan elapsed, int rowCount) {
			this.Command = command;
			this.Elapsed = elapsed;
			this.RowCount = rowCount;
		}
	}
}
