using CheckThat;
using Npgsql;
using Xunit;
using Testcontainers.PostgreSql;
using System.Data;

namespace DataBoss.Data.Npgsql
{
	public class NpgsqlTestDatabase
	{
		readonly PostgreSqlContainer pg;
		readonly Dictionary<string, string> config;
		public NpgsqlTestDatabase() {
			pg = new PostgreSqlBuilder().Build();
			pg.StartAsync().Wait();

			config = pg.GetConnectionString().Split(';').Select(x => x.Split('=')).ToDictionary(x => x[0], x => x[1]);
		}

		public string this[string key] => config[key];
		public string ConnectionString => pg.GetConnectionString();
	}

	public abstract class DbConnectionFixture<TConnection> where TConnection : IDbConnection {
		protected abstract IDbConnection DbConnection { get; }

		[Fact]
		public void parameter_roundtrip() =>
			Check.That(() => (string)DbConnection.ExecuteScalar("select @foo", new { foo = "Hello Npgsql World"}) == "Hello Npgsql World");

		[Fact]
		public void nullable_parameter() => 
			Check.That(() => DbConnection.ExecuteScalar("select @foo", new { foo = (int?)null }) == DBNull.Value);

		[Fact]
		public void null_string() => 
			Check.That(() => DbConnection.ExecuteScalar("select @str", new { str = (string?)null }) == DBNull.Value);

		[Fact]
		public void null_array() => 
			Check.That(() => (int)DbConnection.ExecuteScalar("select case when @str is null then 1 else 0 end", new { str = (string[])null }) == 1);
		[Fact]
		public void null_array_like() => 
			Check.That(() => (int)DbConnection.ExecuteScalar("select case when @str is null then 1 else 0 end", new { str = (List<int>)null }) == 1);

	} 

    public class DataBossNpgsqlConnection_ : DbConnectionFixture<NpgsqlConnection>, IDisposable, IClassFixture<NpgsqlTestDatabase>
	{
		NpgsqlConnection db;

		protected override IDbConnection DbConnection => db;

		public DataBossNpgsqlConnection_(NpgsqlTestDatabase testDb) {
			NpgsqlConnection.ClearAllPools();
			db = new NpgsqlConnection(testDb.ConnectionString);
			db.Open();
		}

		public void Dispose() => db.Dispose();

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

#pragma warning disable 0649
		class IntoRow
		{
			public int @Int;
			public int? NullableInt;
		}
#pragma warning restore 0649
    }

	public class NpgsqlDataBoss : IClassFixture<NpgsqlTestDatabase>
	{
		NpgsqlTestDatabase testDb;

		class DataBossTestConfig : IDataBossConfiguration
		{
			readonly string connectionString;

			public DataBossTestConfig(string connectionString) {
				this.connectionString = connectionString;
			}
			public string Script => throw new NotImplementedException();

			public string DefaultSchema => "public";
			public string Database => "";
			public string Server => "localhost";

			public string GetConnectionString() => connectionString;
			public IDbConnection GetDbConnection() => new NpgsqlConnection(GetConnectionString());

			public IDataBossMigration GetTargetMigration() {
				throw new NotImplementedException();
			}
		}

		public NpgsqlDataBoss(NpgsqlTestDatabase testDb) {
			this.testDb = testDb;
		}

		[Fact]
		public void Initialize() {
			var dataBoss = DataBoss.Create(
				new DataBossTestConfig(testDb.ConnectionString),
				new NullDataBossLog());

			dataBoss.Initialize();
		}

	}
}