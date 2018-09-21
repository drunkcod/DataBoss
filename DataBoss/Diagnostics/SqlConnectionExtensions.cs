using System.Data.SqlClient;
using DataBoss.Data;

namespace DataBoss.Diagnostics
{
	public static class SqlConnectionExtensions
	{
		public static DatabaseInfo GetDatabaseInfo(this SqlConnection connection)
		{
			var reader = DbObjectReader.Create(connection);
			return reader.Single<DatabaseInfo>(@"
				select 
					ServerName = cast(serverproperty('ServerName') as nvarchar(max)),
					ServerVersion = cast(serverproperty('ProductVersion') as nvarchar(max)),
					DatabaseName = db.name,
					DatabaseId = db.database_id,
					CompatibilityLevel = db.compatibility_level
				from sys.databases db where database_id = db_id()");
		}
	}
}
