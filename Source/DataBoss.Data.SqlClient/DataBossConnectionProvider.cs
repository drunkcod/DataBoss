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
		readonly string connectionString;
		readonly ConcurrentDictionary<int, SqlConnection> connections = new();
		readonly ConcurrentDictionary<string, long> accumulatedStats = new();

		int nextConnectionId = 0;
		int resetAt = 0;
		bool statisticsEnabled = false;
		
		public DataBossConnectionProvider(SqlConnectionStringBuilder connectionString) : this(connectionString.ToString()) { }

		public DataBossConnectionProvider(string connectionString) {
			this.connectionString = connectionString;
		}

		public string ConnectionString => connectionString;

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
			db.Disposed += delegate {
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
				cmd.Disposed += SqlCommandExtensions.DisposeConnection;
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
			cmd.AddParameters(args);
			return cmd;
		}

		public int ExecuteNonQuery(string commandText, CommandType commandType = CommandType.Text) {
			using var c = NewAutoCommand(commandText, commandType);
			return c.ExecuteNonQuery();
		}

		public object ExecuteNonQuery<TArgs>(string commandText, TArgs args, CommandType commandType = CommandType.Text) {
			using var c = NewAutoCommand(commandText, args, commandType);
			return c.ExecuteNonQuery();
		}

		public object ExecuteScalar(string commandText, CommandType commandType = CommandType.Text) {
			using var c = NewAutoCommand(commandText, commandType);
			return c.ExecuteScalar();
		}

		public object ExecuteScalar<TArgs>(string commandText, TArgs args, CommandType commandType = CommandType.Text) {
			using var c = NewAutoCommand(commandText, args, commandType);
			return c.ExecuteScalar();
		}

		SqlCommand NewAutoCommand(string commandText, CommandType commandType) =>
			NewCommand(commandText, CommandOptions.DisposeConnection | CommandOptions.OpenConnection, commandType);

		SqlCommand NewAutoCommand<TArgs>(string commandText, TArgs args, CommandType commandType) =>
			NewCommand(commandText, args, CommandOptions.DisposeConnection | CommandOptions.OpenConnection, commandType);

		public SqlDataReader ExecuteReader(string commandText, int? commandTimeout = null) => 
			ExecuteReaderWithCleanup(NewCommand(commandText, CommandOptions.OpenConnection), commandTimeout);

		public SqlDataReader ExecuteReader<TArgs>(string commandText, TArgs args, int? commandTimeout = null) =>
			ExecuteReaderWithCleanup(NewCommand(commandText, args, CommandOptions.OpenConnection), commandTimeout);

		static SqlDataReader ExecuteReaderWithCleanup(SqlCommand c, int? commandTimeout) {
			if (commandTimeout.HasValue)
				c.CommandTimeout = commandTimeout.Value;
			var cleanup = new ReaderCleanup(c);
			c.Connection.StateChange += cleanup.CleanupOnClose;
			try {
				return (cleanup.Reader = c.ExecuteReader(CommandBehavior.CloseConnection));
			} catch {
				c.Connection.Close();
				throw;
			}
		}

		class ReaderCleanup
		{
			public readonly SqlCommand Command;
			public SqlDataReader Reader;

			public ReaderCleanup(SqlCommand command) { this.Command = command; }

			public void CleanupOnClose(object sender, StateChangeEventArgs e) {
				var connection = (SqlConnection)sender;
				if(e.CurrentState == ConnectionState.Closed) {
					Cleanup();
					connection.Dispose();
				}
			}

			void Cleanup() {
				Reader?.Dispose();
				Command.Dispose();
			}
		}

		public int ConnectionsCreated => nextConnectionId - resetAt;
		public int LiveConnections => connections.Count;

		public void SetStatisticsEnabled(bool value) {
			statisticsEnabled = value;
			foreach(var item in connections.Values)
				item.StatisticsEnabled = value;
		}

		public ProviderStatistics RetrieveStatistics() {
			var stats = new Dictionary<string, long>(accumulatedStats) {
				{ "TotalConnectionsCreated", nextConnectionId },
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
			resetAt = nextConnectionId;
			foreach(var item in connections.Values)
				item.ResetStatistics();
		}

		void IDisposable.Dispose() => Cleanup();

		public void Cleanup() =>
			connections.Values.ForEach(DoDispose);

		static void DoDispose(IDisposable x) => x.Dispose();
	}
}
