using System.Collections.Generic;

namespace DataBoss
{
	public struct DataBossQueryBatch
	{
		readonly string batch;

		public DataBossQueryBatch(string batch) {
			this.batch = batch;
		}

		public override string ToString() => batch;
	}

	public interface IDataBossMigration
	{
		DataBossMigrationInfo Info { get; }
		bool HasQueryBatches { get; }
		IEnumerable<DataBossQueryBatch> GetQueryBatches(); 
		IEnumerable<IDataBossMigration> GetSubMigrations();
	}
}