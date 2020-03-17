#if MSSQLCLIENT
namespace DataBoss.Data.MsSql
{
	using Microsoft.Data.SqlClient;
#else
namespace DataBoss.Data
{
	using System.Data.SqlClient;
#endif

	using System;
	using System.Collections;
	using System.Data;
	using System.Data.Common;
	using System.Diagnostics;
	using System.IO;
	using System.Threading;
	using System.Threading.Tasks;

	public class ProfiledSqlConnection : DbConnection, IDataBossConnectionExtras
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
		public override event StateChangeEventHandler StateChange {
			add { inner.StateChange += value; }
			remove { inner.StateChange -= value; }
		}

		protected override DbCommand CreateDbCommand() => new ProfiledSqlCommand(this, inner.CreateCommand());
		protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel) => inner.BeginTransaction(isolationLevel);
		public override void Close() => inner.Close();
		public override void ChangeDatabase(string databaseName) => inner.ChangeDatabase(databaseName);
		public override void Open() => inner.Open();
		public override Task OpenAsync(CancellationToken cancellationToken) => inner.OpenAsync(cancellationToken);

		protected override DbProviderFactory DbProviderFactory => new ProfiledSqlDbProviderFactory();

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

		internal class ProfiledCommandExecutionScope
		{
			readonly ProfiledSqlCommand command;
			readonly Stopwatch stopwatch = Stopwatch.StartNew();

			public ProfiledCommandExecutionScope(ProfiledSqlCommand parent) {
				this.command = parent;
			}

			public void OnExecuted(int rowCount, ProfiledDataReader reader) =>
				command.Connection.OnExecuted(command, stopwatch.Elapsed, rowCount, reader);
		}

		internal ProfiledCommandExecutionScope OnExecuting(ProfiledSqlCommand command) {
			CommandExecuting?.Invoke(this, new ProfiledSqlCommandExecutingEventArgs(command));
			return new ProfiledCommandExecutionScope(command);
		}

		void OnExecuted(ProfiledSqlCommand command, TimeSpan elapsed, int rowCount, ProfiledDataReader reader) =>
			CommandExecuted?.Invoke(this, new ProfiledSqlCommandExecutedEventArgs(command, elapsed, rowCount, reader));

		void IDataBossConnectionExtras.CreateTable(string destinationTable, IDataReader data) =>
			inner.CreateTable(destinationTable, data);

		public static explicit operator SqlConnection(ProfiledSqlConnection self) => self.inner;
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
}
