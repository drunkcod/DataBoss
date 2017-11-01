using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Threading;

namespace DataBoss.Data
{
	public class DataBossConnectionProvider
	{
		public struct ByteSize
		{
			static string[] Unit = new[] { "KiB", "MiB", "GiB", "TiB", "PiB" };
			static long[] Threshold = new[] { 1L << 10, 1L << 20, 1L << 30, 1L << 40, 1L << 50 };
			public readonly long TotalBytes;

			public ByteSize(long totalBytes) { this.TotalBytes = totalBytes; }

			public static implicit operator long(ByteSize size) => size.TotalBytes;

			public override string ToString() {
				var unit = 0;
				for (; unit < Threshold.Length && Threshold[unit] < TotalBytes; ++unit)
					;

				if (unit == 0)
					return TotalBytes.ToString();
				unit -= 1;
				return string.Format("{0:N2} {1}", (1.0 * TotalBytes) / Threshold[unit], Unit[unit]);
			}
		}

		public struct ProviderStatistics : IEnumerable<KeyValuePair<string, long>>
		{
			readonly Dictionary<string, long> stats;

			public ProviderStatistics(Dictionary<string, long> stats) {
				this.stats = stats;
			}

			public ByteSize BytesReceived => new ByteSize(GetOrDefault(nameof(BytesReceived)));
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

		public SqlConnection NewConnection() => NewConnection(connectionString);

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
	}
}
