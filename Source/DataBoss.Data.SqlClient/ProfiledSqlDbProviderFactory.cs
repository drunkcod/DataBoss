#if MSSQLCLIENT
namespace DataBoss.Data.MsSql
{
	using Microsoft.Data.SqlClient;
#else
namespace DataBoss.Data.SqlClient
{
	using System.Data.SqlClient;
#endif

	using System;
	using System.Data.Common;

	public class ProfiledSqlDbProviderFactory : DbProviderFactory
	{
		public override DbConnection CreateConnection() {
			var c = new ProfiledSqlConnection(new SqlConnection());
			ConnectionCreated?.Invoke(c);
			return c;
		}

		public override DbCommand CreateCommand() {
			var c = new ProfiledSqlCommand(new SqlCommand());
			CommandCreated?.Invoke(c);
			return c;
		}

		public Action<ProfiledSqlConnection> ConnectionCreated;
		public Action<ProfiledSqlCommand> CommandCreated;
	}
}
