using System;
using System.Collections.Generic;

namespace DataBoss.Migrations
{
	public class DataBossCompositeMigration : IDataBossMigration
	{
		readonly IDataBossMigration[] migrations;

		public DataBossCompositeMigration(IDataBossMigration[] migrations) {
			this.migrations = migrations;
		}

		public DataBossMigrationInfo Info => throw new NotImplementedException(); 
		public string Path => throw new NotSupportedException();

		public bool HasQueryBatches => false;

		public IEnumerable<DataBossQueryBatch> GetQueryBatches() {
			yield break;
		}

		public IEnumerable<IDataBossMigration> GetSubMigrations() => migrations;
	}
}