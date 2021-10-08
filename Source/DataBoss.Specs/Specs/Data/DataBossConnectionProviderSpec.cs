using CheckThat;
using DataBoss.Testing.SqlServer;
using Xunit;

namespace DataBoss.Data
{
	public class DataBossConnectionProviderSpec : IClassFixture<TemporaryDatabaseFixture>
	{
		DataBossConnectionProvider Connections;

		public DataBossConnectionProviderSpec(TemporaryDatabaseFixture db) {
			Connections = new DataBossConnectionProvider(db.ConnectionString);
		}

		[Fact]
		public void keeps_live_count() {
			using(var db = Connections.NewConnection())
				Check.That(() => Connections.LiveConnections == 1);
			Check.That(() => Connections.LiveConnections == 0, () => Connections.ConnectionsCreated == 1);
		}

		[Fact]
		public void provider_statistics() {
			Connections.SetStatisticsEnabled(true);
			using(var db = Connections.NewConnection()) {
				db.Open();
				db.ExecuteScalar("select 42");
				Check.With(() => Connections.RetrieveStatistics())
					.That(stats => stats["SelectCount"] == 1);
			}
		}

		[Fact]
		public void keeps_stats_for_disposed_connections() {
			Connections.SetStatisticsEnabled(true);
			using(var db = Connections.NewConnection()) {
				db.Open();
				db.ExecuteScalar("select 1");
			}
			using(var db = Connections.NewConnection()) {
				db.Open();
				db.ExecuteScalar("select 2");
			}
			Check.With(() => Connections.RetrieveStatistics())
				.That(stats => stats.SelectCount == 2);
		}

		[Fact]
		public void reset_statistics() {
			Connections.SetStatisticsEnabled(true);
			using(var db = Connections.NewConnection()) {
				db.Open();
				db.ExecuteScalar("select 3");
				Connections.ResetStatistics();
				Check.With(() => Connections.RetrieveStatistics())
					.That(stats => stats["SelectCount"] == 0);
			}
		}

		[Fact]
		public void cleanup() {
			var disposed = false;
			var db = Connections.NewConnection();
			db.Disposed += (_, __) => disposed = true;
			Connections.Cleanup();
			Check.That(
				() => disposed, 
				() => Connections.LiveConnections == 0);
		}

		[Fact]
		public void command_owning_connection() {
			var c = Connections.NewCommand(CommandOptions.DisposeConnection);
			Check.That(() => Connections.LiveConnections == 1);

			c.Dispose();
			Check.That(() => Connections.LiveConnections == 0);
		}
	}
}
