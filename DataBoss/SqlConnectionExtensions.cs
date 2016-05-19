using System.Data.SqlClient;
using DataBoss.Data;
using DataBoss.Diagnostics;
using DataBoss.Util;

namespace DataBoss
{
	public static class SqlConnectionExtensions
	{
		public static SqlCommand CreateCommand(this SqlConnection connection, string cmdText) {
			return new SqlCommand(cmdText, connection);
		}

		public static SqlCommand CreateCommand<T>(this SqlConnection connection, string cmdText, T args) {
			var cmd = new SqlCommand(cmdText, connection);
			cmd.Parameters.AddRange(ToParams.Invoke(args));
			return cmd;
		}

		public static DatabaseInfo GetDatabaseInfo(this SqlConnection connection)
		{
			var reader = new DbObjectReader(connection);
			return reader.Single<DatabaseInfo>(
				@"select 
						ServerName = serverproperty('ServerName'),
						ServerVersion = serverproperty('ProductVersion'),
						DatabaseName = db.name,
						DatabaseId = db.database_id,
						CompatibilityLevel = db.compatibility_level
					from sys.databases db where database_id = db_id()"
				);
		}
	}
}