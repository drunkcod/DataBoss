namespace DataBoss.Data
{
	using System;
	using System.Collections;
	using System.Collections.Generic;
	using System.Linq;

	[Flags]
	public enum CommandOptions
	{
		None = 0,
		DisposeConnection = 1,
		OpenConnection = 2,
	}

	public struct ProviderStatistics : IEnumerable<KeyValuePair<string, long>>
	{
		readonly Dictionary<string, long> stats;

		public ProviderStatistics(Dictionary<string, long> stats) {
			this.stats = stats;
		}

		public ByteSize BytesReceived => new ByteSize(GetOrDefault(nameof(BytesReceived)));
		public ByteSize BytesSent => new ByteSize(GetOrDefault(nameof(BytesSent)));
		public long? ConnectionsCreated => stats.TryGetValue(nameof(ConnectionsCreated), out var found) ? found : (long?)null;
		public long? LiveConnections => stats.TryGetValue(nameof(LiveConnections), out var found) ? found : (long?)null;
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

		public static ProviderStatistics From(IDictionary stats) =>
			new ProviderStatistics(stats.Cast<DictionaryEntry>().ToDictionary(x => (string)x.Key, x => (long)x.Value));
	}
}
