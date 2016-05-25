using System;
using System.Collections.Generic;

namespace DataBoss.Migrations
{
	class DataBossCompositeMigration : IDataBossMigration
	{
		readonly IDataBossMigration[] migrations;

		public DataBossCompositeMigration(IDataBossMigration[] migrations) {
			this.migrations = migrations;
		}

		public DataBossMigrationInfo Info {
			get { throw new NotImplementedException(); }
		}

		public bool HasQueryBatches => false;

		public IEnumerable<DataBossQueryBatch> GetQueryBatches() {
			yield break;
		}

		public IEnumerable<IDataBossMigration> GetSubMigrations() {
			return migrations;
		}
	}
}