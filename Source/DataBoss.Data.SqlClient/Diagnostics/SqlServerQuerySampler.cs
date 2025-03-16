#if MSSQLCLIENT
using DataBoss.Data.MsSql;
using Microsoft.Data.SqlClient;
using SqlConnection = Microsoft.Data.SqlClient.SqlConnection;
using SqlObjectReader = DataBoss.Data.MsSql.SqlObjectReader;
#else
using System.Data.SqlClient;
using SqlConnection = System.Data.SqlClient.SqlConnection;
using SqlObjectReader = DataBoss.Data.SqlClient.SqlObjectReader;
#endif

using System.Collections.Generic;
using DataBoss.Data;

namespace DataBoss.Diagnostics
{
	public enum QuerySampleMode
	{
		ActiveDatabase,
		Global
	}

	public interface ISqlServerQuerySampler
	{
		IEnumerable<QuerySample> TakeSample(QuerySampleMode mode);
	}

	public static class SqlServerQuerySampler
	{
		public static ISqlServerQuerySampler Create(SqlConnection db) => Create(db.GetDatabaseInfo(), SqlObjectReader.Create(db));

		public static ISqlServerQuerySampler Create(DatabaseInfo db, DbObjectReader<SqlCommand, SqlDataReader> reader) {
			if (db.CompatibilityLevel < 90)
				return new SqlServer2000QuerySampler(reader);
			return new SqlServer2005QuerySampler(reader);
		}
	}
}