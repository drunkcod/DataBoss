using CheckThat;
using Npgsql;
using Xunit;

namespace DataBoss.Data.Npgsql
{
	public class NpgsqlTestDatabase : IDisposable
	{
		readonly NpgsqlConnection db;
		public NpgsqlTestDatabase() {
			db = new NpgsqlConnection("Host=localhost;Username=databoss;Password=databoss;Database=postgres");
			db.Open();
			db.ExecuteNonQuery("drop database if exists databoss with(force)");
			db.ExecuteNonQuery("create database databoss");
			db.Close();
		}

		public void Dispose() {
			db.Open();
			db.ExecuteNonQuery("drop database databoss with(force)");
		}
	}

    public class DataBossNpgsqlConnection_ : IDisposable, IClassFixture<NpgsqlTestDatabase>
	{
		NpgsqlConnection db;

		public DataBossNpgsqlConnection_() {
			NpgsqlConnection.ClearAllPools();
			db = new NpgsqlConnection("Host=localhost;Username=databoss;Password=databoss;Database=databoss");
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

		class IntoRow
		{
			public int @Int;
			public int? NullableInt;
		}
    }
}