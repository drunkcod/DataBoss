using System;
using System.Collections.Generic;
using System.Linq;

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
		public bool IsRepeatable => migrations.All(x => x.IsRepeatable);

		public IEnumerable<DataBossQueryBatch> GetQueryBatches() {
			yield break;
		}

		public IEnumerable<IDataBossMigration> GetSubMigrations() => migrations;
	}
}