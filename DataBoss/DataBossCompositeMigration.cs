using System;
using System.Collections.Generic;

namespace DataBoss
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

		public bool HasQueryBatches { get { return false; } }

		public IEnumerable<string> GetQueryBatches() {
			yield break;
		}

		public IEnumerable<IDataBossMigration> GetSubMigrations() {
			return migrations;
		}
	}
}