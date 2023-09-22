using System;
using System.Linq;
using CheckThat;
using DataBoss.Linq;
using Xunit;

namespace DataBoss.Data
{
	public class Query_ : IClassFixture<SqlServerFixture>
	{
		readonly DataBossConnectionProvider Connections;

		public Query_(SqlServerFixture db) {
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

		[Fact]
		public void DbCommand_ExecuteQuery() {
			using var db = Connections.OpenConnection();
			using var cmd = db.CreateCommand();
			Check.That(
				() => cmd.ExecuteQuery<string>("select Value = 'Hello.' union all select null ").ToList().SequenceEqual(new[] { "Hello.", null }),
				() => cmd.ExecuteQuery<int>("select N  = @First union all select @Second", new { First = 1, Second = 2 }).ToList().SequenceEqual(new[] { 1, 2 }),
				() => cmd.ExecuteQuery<DateTime>("select T = cast('2021-03-24 12:58:01' as datetime)").Single() == new DateTime(2021, 3, 24, 12, 58, 1));
		}
	}
}
