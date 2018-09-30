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
	public static class ProfiledSqlConnectionExtensions
	{
		public static ProfiledSqlConnectionTraceWriter StartTrace(this ProfiledSqlConnection self, TextWriter target) => new ProfiledSqlConnectionTraceWriter(self, target);
		public static ProfiledSqlConnectionStatistics StartGatheringQueryStatistics(this ProfiledSqlConnection self) {
			var stats = new ProfiledSqlConnectionStatistics(self) {
				StatisticsEnabled = true
			};
			return stats;
		}
	}

	public class ProfiledSqlConnection : DbConnection, IDbConnectionExtras
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

		public bool StatisticsEnabled {
			get => inner.StatisticsEnabled;
			set => inner.StatisticsEnabled = value;
		}

		public IDictionary RetrieveStatistics() => inner.RetrieveStatistics();
		public void ResetStatistics() => inner.ResetStatistics();

		public event EventHandler<ProfiledSqlCommandExecutingEventArgs> CommandExecuting;
		public event EventHandler<ProfiledSqlCommandExecutedEventArgs> CommandExecuted;
		public event EventHandler<ProfiledBulkCopyStartingEventArgs> BulkCopyStarting;

		protected override DbCommand CreateDbCommand() => new ProfiledSqlCommand(this, inner.CreateCommand());
		protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel) => inner.BeginTransaction(isolationLevel);
		public override void Close() => inner.Close();
		public override void ChangeDatabase(string databaseName) => inner.ChangeDatabase(databaseName);
		public override void Open() => inner.Open();
		public override Task OpenAsync(CancellationToken cancellationToken) => inner.OpenAsync(cancellationToken);

		public void Into(string destinationTable, IDataReader toInsert, DataBossBulkCopySettings settings) {
			inner.CreateTable(destinationTable, toInsert);
			Insert(destinationTable, toInsert, settings);
		}

		public void Insert(string destinationTable, IDataReader toInsert, DataBossBulkCopySettings settings) {
			using(var rows = new ProfiledDataReader(toInsert)) {
				BulkCopyStarting?.Invoke(this, new ProfiledBulkCopyStartingEventArgs(destinationTable, rows));
				inner.Insert(destinationTable, rows, settings);
			}
		}

		internal T Execute<T>(ProfiledSqlCommand command, ExecuteT<T> executeT) {
			var scope = OnExecuting(command);
			scope.OnExecuted(executeT(command.inner, out var r), null);
			return r;
		}

		internal class ExecutionScope
		{
			readonly ProfiledSqlCommand parent;
			readonly Stopwatch stopwatch = Stopwatch.StartNew();

			public ExecutionScope(ProfiledSqlCommand parent) {
				this.parent = parent;
			}

			public void OnExecuted(int rowCount, ProfiledDataReader reader) =>
				((ProfiledSqlConnection)parent.Connection).CommandExecuted?.Invoke(this, new ProfiledSqlCommandExecutedEventArgs(parent, stopwatch.Elapsed, rowCount, reader));

		}

		internal ExecutionScope OnExecuting(ProfiledSqlCommand command) {
			CommandExecuting?.Invoke(this, new ProfiledSqlCommandExecutingEventArgs(command));
			return new ExecutionScope(command);
		}

		void IDbConnectionExtras.CreateTable(string destinationTable, IDataReader data) =>
			inner.CreateTable(destinationTable, data);
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
		public readonly ProfiledDataReader DataReader;
		public readonly TimeSpan Elapsed;
		public readonly int RowCount;

		public ProfiledSqlCommandExecutedEventArgs(ProfiledSqlCommand command, TimeSpan elapsed, int rowCount, ProfiledDataReader reader) {
			this.Command = command;
			this.DataReader = reader;
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
