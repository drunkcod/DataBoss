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

		public static DataBossMigrationScopeContext From(SqlConnection db) =>
			new DataBossMigrationScopeContext(
				db.ConnectionString,
				db.Database,
				string.IsNullOrEmpty(db.DataSource) ? "." : db.DataSource);
	}
}