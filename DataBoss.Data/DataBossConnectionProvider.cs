using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Threading;
using DataBoss.Linq;

namespace DataBoss.Data
{
	[Flags]
	public enum CommandOptions
	{
		DisposeConnection = 1,
		OpenConnection = 2,
	}

	public class DataBossConnectionProvider : IDisposable
	{
		static class CommonOps
		{
			public static readonly Func<SqlCommand, int> ExecuteNonQuery = DelegateUtil.CreateDelegate<SqlCommand, int>(nameof(SqlCommand.ExecuteNonQuery));
			public static readonly Func<SqlCommand, object> ExecuteScalar = DelegateUtil.CreateDelegate<SqlCommand, object>(nameof(SqlCommand.ExecuteScalar));
			public static readonly Action<SqlConnection> Dispose = DelegateUtil.CreateDelegate<SqlConnection>(nameof(SqlConnection.Dispose));
		}

		static readonly EventHandler DisposeConnection = (sender, _) => ((SqlCommand)sender).Connection.Dispose();

		public struct ProviderStatistics : IEnumerable<KeyValuePair<string, long>>
		{
			readonly Dictionary<string, long> stats;

			public ProviderStatistics(Dictionary<string, long> stats) {
				this.stats = stats;
			}

			public ByteSize BytesReceived => new ByteSize(GetOrDefault(nameof(BytesReceived)));
			public ByteSize BytesSent => new ByteSize(GetOrDefault(nameof(BytesSent)));
			public long ConnectionsCreated => GetOrDefault(nameof(ConnectionsCreated));
			public long LiveConnections => GetOrDefault(nameof(LiveConnections));
			public long SelectCount => GetOrDefault(nameof(SelectCount));
			public long SelectRows => GetOrDefault(nameof(SelectRows));
			public TimeSpan ExecutionTime => TimeSpan.FromMilliseconds(GetOrDefault(nameof(ExecutionTime)));

			public long this[string key] => stats[key];
			public IEnumerator<KeyValuePair<string, long>> GetEnumerator() => stats.GetEnumerator();
			IEnumerator IEnumerable.GetEnumerator() => stats.GetEnumerator();

			long GetOrDefault(string key) {
				stats.TryGetValue(key, out var value);
				return value;
			}
		}

		readonly string connectionString;
		readonly ConcurrentDictionary<int, SqlConnection> connections = new ConcurrentDictionary<int, SqlConnection>();
		readonly ConcurrentDictionary<string, long> accumulatedStats = new ConcurrentDictionary<string, long>();
		int nextConnectionId = 0;
		bool statisticsEnabled = false;
		
		public DataBossConnectionProvider(SqlConnectionStringBuilder connectionString) : this(connectionString.ToString()) { }
		public DataBossConnectionProvider(string connectionString) {
			this.connectionString = connectionString;
		}

		public SqlConnection OpenConnection() => OpenConnection(connectionString);

		public SqlConnection OpenConnection(string connectionString) {
			var c = NewConnection(connectionString);
			c.Open();
			return c;
		}

		public SqlConnection NewConnection() => NewConnection(connectionString);
		public ProfiledSqlConnection NewProfiledConnection() => new ProfiledSqlConnection(NewConnection());

		public SqlConnection NewConnection(string connectionString) {
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
			ToParams.AddTo(cmd, args);
			return cmd;
		}

		public int ExecuteNonQuery(string commandText, CommandType commandType = CommandType.Text) =>
			Execute(commandText, commandType, CommonOps.ExecuteNonQuery);

		public object ExecuteScalar(string commandText, CommandType commandType = CommandType.Text) => 
			Execute(commandText, commandType, CommonOps.ExecuteScalar);

		T Execute<T>(string commandText, CommandType commandType, Func<SqlCommand, T> @do) {
			using (var c = NewCommand(commandText, CommandOptions.DisposeConnection | CommandOptions.OpenConnection, commandType))
				return @do(c);
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
			connections.Values.ForEach(CommonOps.Dispose);
	}
}
