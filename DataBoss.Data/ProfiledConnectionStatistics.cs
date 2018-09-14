using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace DataBoss.Data
{
	public class ProfiledSqlConnectionStatistics : IEnumerable<ProfiledCommandStatistics>
	{
		readonly Dictionary<string, List<ProfiledCommandCall>> seenQueries = new Dictionary<string, List<ProfiledCommandCall>>();
		readonly ProfiledSqlConnection connection;
		bool enabled;

		public ProfiledSqlConnectionStatistics(ProfiledSqlConnection connection) {
			this.connection = connection;
			this.StatisticsEnabled = true;
		}

		public ProviderStatistics? ProviderStatistics =>
			!connection.StatisticsEnabled ? (ProviderStatistics?)null : Data.ProviderStatistics.From(connection.RetrieveStatistics());

		public bool StatisticsEnabled {
			get => enabled;
			set {
				if (value == enabled)
					return;
				enabled = value;
				if (value) {
					connection.CommandExecuted += OnCommandExecuted;

				}
				else {
					connection.CommandExecuted -= OnCommandExecuted;
				}
			}
		}

		public void Clear() => seenQueries.Clear();

		void OnCommandExecuted(object _, ProfiledSqlCommandExecutedEventArgs e) {
			AggregateStats(e.Command, e.Elapsed, Math.Max(0, e.RowCount), true);
			if (e.DataReader != null)
				e.DataReader.Closed += (__, ee) => AggregateStats(e.Command, ee.Elapsed, ee.RowCount, false);
		}

		void AggregateStats(ProfiledSqlCommand command, TimeSpan elapsed, int rowCount, bool commandExecuted) {
			var p = GetParameters(command);
			if (seenQueries.TryGetValue(command.CommandText, out var found)) {
				found.Add(new ProfiledCommandCall(commandExecuted, rowCount, elapsed, p));
			}
			else {
				var calls = new List<ProfiledCommandCall>();
				calls.Add(new ProfiledCommandCall(commandExecuted, rowCount, elapsed, p));
				seenQueries.Add(command.CommandText, calls);
			}
		}

		public IEnumerator<ProfiledCommandStatistics> GetEnumerator() => seenQueries
			.Select(x => new ProfiledCommandStatistics(
				x.Key,
				x.Value.Sum(y => y.RowCount),
				x.Value)).GetEnumerator();

		IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

		ProfiledCommandParameter[] GetParameters(ProfiledSqlCommand command) {
			var p = new ProfiledCommandParameter[command.Parameters.Count];
			for (var i = 0; i != p.Length; ++i) {
				var x = command.Parameters[i];
				p[i] = new ProfiledCommandParameter(x.ParameterName, DataBossDbType.ToDataBossDbType(x), x.Value);
			}
			return p;
		}
	}

	public class ProfiledCommandStatistics
	{
		public string CommandText;
		public TimeSpan TotalElapsed => Calls.Aggregate(TimeSpan.Zero, (acc, x) => acc + x.Elapsed);
		public int CallCount => Calls.Count(x => x.CommandExecuted);
		public int RowCount => Calls.Sum(x => x.RowCount);
		public readonly IReadOnlyList<ProfiledCommandCall> Calls;

		public ProfiledCommandStatistics(string commandText, int rowCount, IReadOnlyList<ProfiledCommandCall> calls) {
			this.CommandText = commandText;
			this.Calls = calls;
		}
	}

	public class ProfiledCommandCall
	{
		public bool CommandExecuted;
		public int RowCount;
		public TimeSpan Elapsed;
		public ProfiledCommandParameter[] Parameters;

		public ProfiledCommandCall(bool commandExecuted, int rowCount, TimeSpan elapsed, ProfiledCommandParameter[] parameters) {
			this.CommandExecuted = commandExecuted;
			this.RowCount = rowCount;
			this.Elapsed = elapsed;
			this.Parameters = parameters;
		}
	}

	public struct ProfiledCommandParameter
	{
		public readonly string ParameterName;
		public readonly DataBossDbType ParameterType;
		public readonly object Value;

		public ProfiledCommandParameter(string name, DataBossDbType type, object value) {
			this.ParameterName = name;
			this.ParameterType = type;
			this.Value = value;
		}

		public override string ToString() => $"{ParameterName} {ParameterType} = {ParameterType.FormatValue(Value)}";
	}
}
