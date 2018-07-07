using System.Collections.Generic;
using System.Data.SqlClient;
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
		public static ISqlServerQuerySampler Create(SqlConnection db) => Create(db.GetDatabaseInfo(), new DbObjectReader<SqlCommand, SqlDataReader>(db.CreateCommand));

		public static ISqlServerQuerySampler Create(DatabaseInfo db, DbObjectReader<SqlCommand, SqlDataReader> reader) {
			if(db.CompatibilityLevel < 90)
				return new SqlServer2000QuerySampler(reader);
			return new SqlServer2005QuerySampler(reader);
		}
	}
}