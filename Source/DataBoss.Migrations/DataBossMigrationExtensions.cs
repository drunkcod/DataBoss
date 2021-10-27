using System.Collections.Generic;
using System.Linq;

namespace DataBoss
{
    public static class DataBossMigrationExtensions
    {
        public static IEnumerable<IDataBossMigration> Flatten(this IDataBossMigration migration) {
            yield return migration;
            foreach (var item in migration.GetSubMigrations().SelectMany(Flatten))
                yield return item;
        }
    }
}