using System.Data.SqlClient;
using DataBoss.Testing.SqlServer;

namespace DataBoss.Data
{
	public class TemporaryDatabaseFixture
	{
		public readonly string ConnectionString;

		public TemporaryDatabaseFixture() {
			ConnectionString = SqlServerTestDb.Create().ConnectionString;
			SqlServerTestDb.RegisterForAutoCleanup();
		}

		public SqlConnection Open() {
			var c = new SqlConnection(ConnectionString);
			c.Open();
			return c;
		}
	}
}
