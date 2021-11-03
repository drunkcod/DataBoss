using DataBoss.Linq;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace DataBoss.Migrations
{
	public class DataBossExternalCommandMigration : IDataBossMigration
	{
		readonly Func<TextReader> getReader;
 
		public DataBossExternalCommandMigration(string path, Func<TextReader> getReader, DataBossMigrationInfo info, bool isRepeatable) {
			this.Path = path;
			this.getReader = getReader;
			this.Info = info;
			this.IsRepeatable = isRepeatable;
		}

		public DataBossMigrationInfo Info { get; }
		public string Path { get;}
		public bool IsRepeatable { get; }

		public bool HasQueryBatches => true;

		public IEnumerable<DataBossQueryBatch> GetQueryBatches() => 
			getReader.Select(x => DataBossQueryBatch.ExternalCommand(x, Path));

		IEnumerable<IDataBossMigration> IDataBossMigration.GetSubMigrations() => 
			Enumerable.Empty<IDataBossMigration>();
	}
}
