using CheckThat;
using Npgsql;
using Xunit;

namespace DataBoss.Data.Npgsql
{
    public class DataBossNpgsqlConnection_
	{
		[Fact]
		public void parameter_roundtrip() {
			using var db = new NpgsqlConnection("Host=localhost;Username=databoss;Password=databoss;Database=postgres");

			db.Open();

			Check.That(() => (string)db.ExecuteScalar("select @foo", new { foo = "Hello Npgsql World"}) == "Hello Npgsql World");
		}
    }
}