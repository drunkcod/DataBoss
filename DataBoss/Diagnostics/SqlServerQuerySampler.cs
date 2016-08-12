using System.Collections.Generic;
using DataBoss.Data;

namespace DataBoss.Diagnostics
{
	public interface ISqlServerQuerySampler
	{
		IEnumerable<QuerySample> TakeSample(); 
	}

	public static class SqlServerQuerySampler
	{
		public static ISqlServerQuerySampler Create(DatabaseInfo db, DbObjectReader reader) {
			if(db.CompatibilityLevel < 90)
				return new SqlServer2000QuerySampler(reader);
			return new SqlServer2005QuerySampler(reader);
		}
	}
}