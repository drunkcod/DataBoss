using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using DataBoss.Data.Scripting;

namespace DataBoss.Data
{
	public class ProfiledSqlConnection : DbConnection
	{
		readonly SqlConnection inner;

		public ProfiledSqlConnection(SqlConnection inner) {
			this.inner = inner;
		}

		public ProfiledSqlConnectionTraceWriter StartTrace(TextWriter target) => new ProfiledSqlConnectionTraceWriter(this, target);

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

		public event EventHandler<ProfiledBulkCopyStartingEventArgs> BulkCopyStarting;

		protected override DbCommand CreateDbCommand() => new ProfiledSqlCommand(this, inner.CreateCommand());
		protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel) => inner.BeginTransaction(isolationLevel);
		public override void Close() => inner.Close();
		public override void ChangeDatabase(string databaseName) => inner.ChangeDatabase(databaseName);
		public override void Open() => inner.Open();
		public override Task OpenAsync(CancellationToken cancellationToken) => inner.OpenAsync(cancellationToken);

		public static void Into(ProfiledSqlConnection connection, string destinationTable, IDataReader toInsert, DataBossBulkCopySettings settings) {
			var scripter = new DataBossScripter();
			connection.ExecuteNonQuery(scripter.ScriptTable(destinationTable, toInsert));
			connection.Insert(destinationTable, toInsert, settings);
		}

		void Insert(string destinationTable, IDataReader toInsert, DataBossBulkCopySettings settings) {
			using(var rows = new ProfiledDataReader(toInsert)) {
				BulkCopyStarting?.Invoke(this, new ProfiledBulkCopyStartingEventArgs(destinationTable, rows));
				inner.Insert(destinationTable, rows, settings);
			}
		}

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

			public void OnReaderClosed(object sender, ProfiledDataReaderClosedEventArgs e) =>
				((ProfiledSqlConnection)parent.Connection).ReaderClosed?.Invoke(this, new ProfiledSqlCommandExecutedEventArgs(parent, stopwatch.Elapsed, e.RowCount));
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

	public class ProfiledBulkCopyStartingEventArgs : EventArgs 
	{
		public readonly string DestinationTable;
		public readonly ProfiledDataReader Rows;

		public ProfiledBulkCopyStartingEventArgs(string destinationTable, ProfiledDataReader rows) {
			this.DestinationTable = destinationTable;
			this.Rows = rows;
		}
	}
}
