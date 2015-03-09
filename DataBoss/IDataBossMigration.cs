using System.Collections.Generic;

namespace DataBoss
{
	public interface IDataBossMigration
	{
		DataBossMigrationInfo Info { get; }
		bool HasQueryBatches { get; }
		IEnumerable<string> GetQueryBatches(); 
		IEnumerable<IDataBossMigration> GetSubMigrations();
	}
}