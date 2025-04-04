#if MSSQLCLIENT
namespace DataBoss.Data.MsSql
{
	using Microsoft.Data.SqlClient;
#else
namespace DataBoss.Data.SqlClient
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

	public class ProfiledSqlConnection : DbConnection, IDataBossConnection
	{
		readonly SqlConnection inner;

		public ProfiledSqlConnection(SqlConnection inner) {
			this.inner = inner;
		}

		public ISqlDialect Dialect => MsSqlDialect.Instance;

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

		IDbCommand IDataBossConnection.CreateCommand() =>
			new ProfiledSqlCommand(this, inner.CreateCommand());

		IDbCommand IDataBossConnection.CreateCommand(string cmdText) =>
			new ProfiledSqlCommand(this, inner.CreateCommand(cmdText));

		IDbCommand IDataBossConnection.CreateCommand<T>(string cmdText, T args) =>
			new ProfiledSqlCommand(this, inner.CreateCommand(cmdText, args));

		IDbCommand IDataBossConnection.CreateCommand(string cmdText, object args) =>
			new ProfiledSqlCommand(this, inner.CreateCommand(cmdText, args));

		protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel) => inner.BeginTransaction(isolationLevel);
		IDbTransaction IDataBossConnection.BeginTransaction() => inner.BeginTransaction();
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
			using var rows = new ProfiledDataReader(toInsert, MsSqlDialect.Scripter);
			BulkCopyStarting?.Invoke(this, new ProfiledBulkCopyStartingEventArgs(destinationTable, rows));
			inner.Insert(destinationTable, rows, settings);
		}

		public async Task InsertAsync(string destinationTable, DbDataReader toInsert, DataBossBulkCopySettings settings, CancellationToken cancellationToken) {
			using var rows = new ProfiledDataReader(toInsert, MsSqlDialect.Scripter);
			BulkCopyStarting?.Invoke(this, new ProfiledBulkCopyStartingEventArgs(destinationTable, rows));
			await inner.InsertAsync(destinationTable, rows, settings, cancellationToken);
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

		void IDataBossConnection.CreateTable(string destinationTable, IDataReader data) =>
			inner.CreateTable(destinationTable, data);

		public static explicit operator SqlConnection(ProfiledSqlConnection self) => self.inner;

		public void EnsureDatabase() => new DataBossSqlConnection(inner).EnsureDatabase();
		public int GetTableVersion(string tableName) => new DataBossSqlConnection(inner).GetTableVersion(tableName);
		public void SetTableVersion(string tableName, int version) => new DataBossSqlConnection(inner).SetTableVersion(tableName, version);

		public string GetDefaultSchema() => new DataBossSqlConnection(inner).GetDefaultSchema();
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
		public static ProfiledSqlConnectionTraceWriter StartTrace(this ProfiledSqlConnection self, TextWriter target) => new(self, target);
		public static ProfiledSqlConnectionStatistics StartGatheringQueryStatistics(this ProfiledSqlConnection self) => new(self) {
			StatisticsEnabled = true
		};
	}
}
