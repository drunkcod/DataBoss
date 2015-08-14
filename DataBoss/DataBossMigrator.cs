using System;
using System.Collections.Generic;

namespace DataBoss
{
	public class DataBossMigrator 
	{
		readonly Func<DataBossMigrationInfo, IDataBossMigrationScope> scopeFactory;

		public DataBossMigrator(Func<DataBossMigrationInfo, IDataBossMigrationScope> scopeFactory) {
			this.scopeFactory = scopeFactory;
		}

		public bool Apply(IDataBossMigration migration) {			
			var scope = scopeFactory(migration.Info);
			try {
				scope.Begin(migration.Info);
				foreach(var query in migration.GetQueryBatches())
					scope.Execute(query);
				return !scope.IsFaulted;
			} finally {
				scope.Done();
			}
		}

		public void ApplyRange(IEnumerable<IDataBossMigration> migrations) {
			foreach (var item in migrations)
				if(!Apply(item))
					break;
		}
	}
}
