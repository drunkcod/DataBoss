using System.Collections.Generic;
using DataBoss.Migrations;

namespace DataBoss
{
	public interface IDataBossMigration
	{
		DataBossMigrationInfo Info { get; }
		string Path { get; }
		bool HasQueryBatches { get; }
		bool IsRepeatable { get; }
		IEnumerable<DataBossQueryBatch> GetQueryBatches(); 
		IEnumerable<IDataBossMigration> GetSubMigrations();
	}
}