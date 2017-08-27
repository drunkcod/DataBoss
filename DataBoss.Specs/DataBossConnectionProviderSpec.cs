using Cone;
using DataBoss.Testing;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Threading;

namespace DataBoss.Specs
{
	public class DataBossConnectionProvider
	{
		readonly string connectionString;
		readonly ConcurrentDictionary<int, SqlConnection> connections = new ConcurrentDictionary<int, SqlConnection>();
		int nextConnectionId = 0;
		bool statisticsEnabled = false;
		
		public DataBossConnectionProvider(SqlConnectionStringBuilder connectionString) : this(connectionString.ToString()) { }
		public DataBossConnectionProvider(string connectionString) {
			this.connectionString = connectionString;
		}

		public SqlConnection NewConnection() {
			var db = new SqlConnection(connectionString) {
				StatisticsEnabled = statisticsEnabled,
			};
			var id = Interlocked.Increment(ref nextConnectionId);
			connections.TryAdd(id, db);
			db.Disposed += (s, e) => connections.TryRemove(id, out var ignored);
			return db;
		}

		public int ConnectionsCreated => nextConnectionId;
		public int LiveConnections => connections.Count;

		public void SetStatisticsEnabled(bool value) {
			statisticsEnabled = value;
			foreach(var item in connections)
				item.Value.StatisticsEnabled = value;
		}

		public Dictionary<string, long> RetreiveStatistics() {
			var stats = new Dictionary<string, long>();
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
			return stats;
		}
	}

	[Describe(typeof(DataBossConnectionProvider))]
	public class DataBossConnectionProviderSpec
	{
		SqlConnectionStringBuilder connectionString;
		DataBossConnectionProvider connections;

		[BeforeAll]
		public void EnsureTestInstance() {
			connectionString = DatabaseSetup.GetTemporaryInstance(nameof(DataBossConnectionProvider) + " Tests");
			DatabaseSetup.RegisterForAutoCleanup();
		}

		[BeforeEach]
		public void given_a_connection_provider() {
			connections = new DataBossConnectionProvider(connectionString);
		}

		public void keeps_live_count() {
			using(var db = connections.NewConnection())
				Check.That(() => connections.LiveConnections == 1);
			Check.That(() => connections.LiveConnections == 0, () => connections.ConnectionsCreated == 1);
		}

		public void provider_statistics() {
			connections.SetStatisticsEnabled(true);
			using(var db = connections.NewConnection()) {
				db.Open();
				db.ExecuteScalar("select 42");
				Check.With(() => connections.RetreiveStatistics())
					.That(stats => stats["SelectCount"] == 1);

			}
		}
	}
}
