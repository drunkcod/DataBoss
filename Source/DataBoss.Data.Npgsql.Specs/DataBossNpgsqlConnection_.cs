using CheckThat;
using Npgsql;
using Xunit;
using Testcontainers.PostgreSql;
using DataBoss.Testing.SqlServer;

namespace DataBoss.Data.Npgsql
{

	public class ToParams_NpgsqlCommand : ToParamsFixture<NpgsqlCommand, NpgsqlParameter>
	{
		protected override NpgsqlCommand NewCommand() => new NpgsqlCommand();
		protected override ISqlDialect SqlDialect => NpgsqlDialect.Instance;

	}

	public class NpgsqlTestDatabase : IDisposable
	{
		readonly PostgreSqlContainer pg;
		readonly NpgsqlConnection db;
		public NpgsqlTestDatabase() {
			pg = new PostgreSqlBuilder().Build();
			pg.StartAsync().Wait();
			db = new NpgsqlConnection(pg.GetConnectionString());
			db.Open();
			db.ExecuteNonQuery("drop database if exists databoss with(force)");
			db.ExecuteNonQuery("create database databoss");
			db.Close();
		}

		public void Dispose() {
			db.Open();
			db.ExecuteNonQuery("drop database databoss with(force)");
		}

		public string ConnectionString => pg.GetConnectionString();
	}

    public class DataBossNpgsqlConnection_ : IDisposable, IClassFixture<NpgsqlTestDatabase>
	{
		NpgsqlConnection db;

		public DataBossNpgsqlConnection_(NpgsqlTestDatabase testDb) {
			NpgsqlConnection.ClearAllPools();
			db = new NpgsqlConnection(testDb.ConnectionString);
			db.Open();
		}

		public void Dispose() => db.Dispose();

		[Fact]
		public void parameter_roundtrip() =>
			Check.That(() => (string)db.ExecuteScalar("select @foo", new { foo = "Hello Npgsql World"}) == "Hello Npgsql World");

		[Fact]
		public void Into_basic_types() {
			db.ExecuteNonQuery("drop table if exists my_table");
			db.Into("my_table", SequenceDataReader.Items(new {
				@Int = 1,
				NullableInt = (int?)null,
			}));

			var row = db.Query<IntoRow>("select * from my_table").Single();
			Check.That(
				() => row.Int == 1,
				() => row.NullableInt.HasValue == false);

		}

		[Fact]
		public void Query_jsonb_parameter() {
			var input = new[] {
				KeyValuePair.Create(1, "One"),
				KeyValuePair.Create(2, "Two"),
			};

			Check.With(() => db.Query<KeyValuePair<int, string>>(@"
				select (value->'Key')::int as key, value->>'Value' as value
				from jsonb_array_elements(:input)", new { input = NpgsqlDialect.Jsonb(input) }, false).ToList())
			.That(
				xs => xs.Count == input.Length,
				xs => xs[0].Key == input[0].Key,
				xs => xs[0].Value == input[0].Value);
		}

		[Fact]
		public void Query_IEnumerableOfT_parameter() {
			var values = new List<int> { 1, 2, 3 };
			var r = db.Query((int value) => value, "select * from unnest(:values) as xs(value)", new { values });

			Check.That(() => r.SequenceEqual(values));
		}


		class IntoRow
		{
			public int @Int;
			public int? NullableInt;
		}
    }
}