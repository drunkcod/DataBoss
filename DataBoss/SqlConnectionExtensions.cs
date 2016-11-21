using System.Data.SqlClient;
using DataBoss.Data;
using DataBoss.Diagnostics;

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

		public static object ExecuteScalar(this SqlConnection connection, string cmdText) {
			using(var q = connection.CreateCommand(cmdText))
				return q.ExecuteScalar();
		}

		public static object ExecuteScalar<T>(this SqlConnection connection, string cmdText, T args) {
			using(var q = CreateCommand(connection, cmdText, args))
				return q.ExecuteScalar();
		}

		public static object ExecuteNonQuery(this SqlConnection connection, string cmdText) {
			using(var q = connection.CreateCommand(cmdText))
				return q.ExecuteNonQuery();
		}

		public static object ExecuteNonQuery<T>(this SqlConnection connection, string cmdText, T args) {
			using(var q = CreateCommand(connection, cmdText, args))
				return q.ExecuteNonQuery();
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