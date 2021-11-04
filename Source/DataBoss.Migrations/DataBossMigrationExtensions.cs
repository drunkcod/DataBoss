using System.Collections.Generic;

namespace DataBoss
{
	public static class DataBossMigrationExtensions
    {
        public static IEnumerable<IDataBossMigration> Flatten(this IDataBossMigration migration) {
            yield return migration;
            foreach(var child in migration.GetSubMigrations())
			foreach(var item in Flatten(child))
                yield return item;
        }
    }
}