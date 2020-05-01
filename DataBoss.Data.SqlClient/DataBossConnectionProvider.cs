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
	using System.Collections.Concurrent;
	using System.Collections.Generic;
	using System.Data;
	using System.Linq;
	using System.Threading;
	using DataBoss.Linq;

	public class DataBossConnectionProvider : IDisposable
	{
		static readonly EventHandler DisposeConnection = (sender, _) => ((SqlCommand)sender).Connection.Dispose();

		readonly string connectionString;
		readonly ConcurrentDictionary<int, SqlConnection> connections = new ConcurrentDictionary<int, SqlConnection>();
		readonly ConcurrentDictionary<string, long> accumulatedStats = new ConcurrentDictionary<string, long>();
		int nextConnectionId = 0;
		bool statisticsEnabled = false;
		
		public DataBossConnectionProvider(SqlConnectionStringBuilder connectionString) : this(connectionString.ToString()) { }
		public DataBossConnectionProvider(string connectionString) {
			this.connectionString = connectionString;
		}

		public SqlConnection OpenConnection() {
			var c = NewConnection(connectionString);
			c.Open();
			return c;
		}

		public SqlConnection NewConnection() => NewConnection(connectionString);
		public ProfiledSqlConnection NewProfiledConnection() => new ProfiledSqlConnection(NewConnection());

		SqlConnection NewConnection(string connectionString) {
			var db = new SqlConnection(connectionString) {
				StatisticsEnabled = statisticsEnabled,
			};
			var id = Interlocked.Increment(ref nextConnectionId);
			connections.TryAdd(id, db);
			db.Disposed += (s, e) => {
				connections.TryRemove(id, out var dead);
				var stats = dead.RetrieveStatistics().GetEnumerator();
				while(stats.MoveNext())
					accumulatedStats.AddOrUpdate((string)stats.Key, (long)stats.Value, (_, acc) => acc + (long)stats.Value);
			};
			return db;
		}

		public SqlCommand NewCommand(CommandOptions options) => NewCommand(options, CommandType.Text);

		public SqlCommand NewCommand(string commandText, CommandOptions options) => NewCommand(commandText, options, CommandType.Text);

		public SqlCommand NewCommand(CommandOptions options, CommandType commandType) {
			var cmd = new SqlCommand {
				Connection = NewConnection(),
				CommandType = commandType,
			};
			if((options & CommandOptions.DisposeConnection) != 0)
				cmd.Disposed += DisposeConnection;
			if((options & CommandOptions.OpenConnection) != 0)
				cmd.Connection.Open();
			return cmd;
		}

		public SqlCommand NewCommand(string commandText, CommandOptions options, CommandType commandType) {
			var cmd = NewCommand(options, commandType);
			cmd.CommandText = commandText;
			return cmd;
		}

		public SqlCommand NewCommand<T>(string commandText, T args, CommandOptions options) =>
			NewCommand(commandText, args, options, CommandType.Text);

		public SqlCommand NewCommand<T>(string commandText, T args, CommandOptions options, CommandType commandType) {
			var cmd = NewCommand(commandText, options, commandType);
			MsSqlDialect.AddTo(cmd, args);
			return cmd;
		}

		public int ExecuteNonQuery(string commandText, CommandType commandType = CommandType.Text) =>
			Execute(commandText, commandType, DbOps<SqlCommand, SqlDataReader>.ExecuteNonQuery);

		public object ExecuteNonQuery<TArgs>(string commandText, TArgs args, CommandType commandType = CommandType.Text) =>
			Execute(commandText, args, commandType, DbOps<SqlCommand, SqlDataReader>.ExecuteNonQuery);

		public object ExecuteScalar(string commandText, CommandType commandType = CommandType.Text) => 
			Execute(commandText, commandType, DbOps<SqlCommand, SqlDataReader>.ExecuteScalar);
		public object ExecuteScalar<TArgs>(string commandText, TArgs args, CommandType commandType = CommandType.Text) =>
			Execute(commandText, args, commandType, DbOps<SqlCommand, SqlDataReader>.ExecuteScalar);

		T Execute<T>(string commandText, CommandType commandType, Func<SqlCommand, T> @do) {
			using (var c = NewCommand(commandText, CommandOptions.DisposeConnection | CommandOptions.OpenConnection, commandType))
				return @do(c);
		}

		T Execute<T,TArgs>(string commandText, TArgs args, CommandType commandType, Func<SqlCommand, T> @do) {
			using (var c = NewCommand(commandText, args, CommandOptions.DisposeConnection | CommandOptions.OpenConnection, commandType))
				return @do(c);
		}

		public SqlDataReader ExecuteReader(string commandText, int? commandTimeout = null) => 
			ExecuteReaderWithCleanup(NewCommand(commandText, CommandOptions.None), commandTimeout);

		public SqlDataReader ExecuteReader<TArgs>(string commandText, TArgs args, int? commandTimeout = null) =>
			ExecuteReaderWithCleanup(NewCommand(commandText, args, CommandOptions.None), commandTimeout);

		static SqlDataReader ExecuteReaderWithCleanup(SqlCommand c, int? commandTimeout) {
			if (commandTimeout.HasValue)
				c.CommandTimeout = commandTimeout.Value;
			c.Connection.Open();
			var r = c.ExecuteReader(CommandBehavior.CloseConnection);
			c.Connection.StateChange += new ReaderCleanup(c, r).CleanupOnClose;
			return r;
		}

		class ReaderCleanup
		{
			SqlCommand command;
			IDisposable reader;

			public ReaderCleanup(SqlCommand command, SqlDataReader reader) {
				this.command = command;
				this.reader = reader;
			}

			public void CleanupOnClose(object sender, StateChangeEventArgs e) {
				var connection = (SqlConnection)sender;
				if(e.CurrentState == ConnectionState.Closed) {
					connection.StateChange -= CleanupOnClose;
					Cleanup();
				}
			}

			void Cleanup() {
				reader.Dispose();
				command.Dispose();
				reader = null;
				command = null;
			}
		}

		public int ConnectionsCreated => nextConnectionId;
		public int LiveConnections => connections.Count;

		public void SetStatisticsEnabled(bool value) {
			statisticsEnabled = value;
			foreach(var item in connections.Values)
				item.StatisticsEnabled = value;
		}

		public ProviderStatistics RetrieveStatistics() {
			var stats = new Dictionary<string, long>(accumulatedStats) {
				{ nameof(ConnectionsCreated), ConnectionsCreated },
				{ nameof(LiveConnections), LiveConnections },
			};
			var connectionStats = Array.ConvertAll(
				connections.Values.ToArray(),
				x => x.RetrieveStatistics());
			foreach(var item in connectionStats) {
				var itemStats = item.GetEnumerator();
				while(itemStats.MoveNext()) {
					var key = (string)itemStats.Key;
					stats.TryGetValue(key, out var found);
					stats[key] = found + (long)itemStats.Value;
				}
			}
			return new ProviderStatistics(stats);
		}

		public void ResetStatistics() { 
			accumulatedStats.Clear();
			foreach(var item in connections.Values)
				item.ResetStatistics();
		}

		void IDisposable.Dispose() => Cleanup();

		public void Cleanup() =>
			connections.Values.ForEach(DoDispose);

		static void DoDispose(IDisposable x) => x.Dispose();
	}
}
