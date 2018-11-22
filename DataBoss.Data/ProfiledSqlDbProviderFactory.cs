using System;
using System.Data.Common;
using System.Data.SqlClient;

namespace DataBoss.Data
{
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
