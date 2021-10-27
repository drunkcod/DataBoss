using System.Data.SqlClient;

namespace DataBoss.Migrations
{
	public class DataBossMigrationScopeContext
	{
		public readonly string ConnectionString;
		public readonly string Database;
		public readonly string Server;

		public DataBossMigrationScopeContext(string connectionString, string database, string server) {
			this.ConnectionString = connectionString;
			this.Database = database;
			this.Server = server;
		}

		public static DataBossMigrationScopeContext From(string connectionString) {
			var cs = new SqlConnectionStringBuilder(connectionString);
			return new DataBossMigrationScopeContext(
				connectionString,
				cs.InitialCatalog,
				string.IsNullOrEmpty(cs.DataSource) ? "." : cs.DataSource);
		}
	}
}