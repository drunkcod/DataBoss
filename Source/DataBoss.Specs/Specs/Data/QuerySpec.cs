using System;
using System.Linq;
using CheckThat;
using Xunit;

namespace DataBoss.Data
{
	public class QuerySpec : IClassFixture<TemporaryDatabaseFixture>
	{
		DataBossConnectionProvider Connections;

		public QuerySpec(TemporaryDatabaseFixture db) {
			Connections = new DataBossConnectionProvider(db.ConnectionString);
		}

		[Fact]
		public void query_scalar() {
			using var db = Connections.OpenConnection();
			Check.That(
				() => db.Query<string>("select Value = 'Hello.' union all select null ").SequenceEqual(new[] { "Hello.", null }),
				() => db.Query<int>("select N  = 1 union all select 2").SequenceEqual(new[] { 1, 2 }),
				() => db.Query<DateTime>("select T = cast('2021-03-24 12:58:01' as datetime)").Single() == new DateTime(2021, 3, 24, 12, 58, 1));
		}
	}
}
