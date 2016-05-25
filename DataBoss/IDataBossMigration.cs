using System.Collections.Generic;
using DataBoss.Migrations;

namespace DataBoss
{
	public interface IDataBossMigration
	{
		DataBossMigrationInfo Info { get; }
		bool HasQueryBatches { get; }
		IEnumerable<DataBossQueryBatch> GetQueryBatches(); 
		IEnumerable<IDataBossMigration> GetSubMigrations();
	}
}