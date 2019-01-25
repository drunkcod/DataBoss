using DataBoss.Linq;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;

namespace DataBoss.Migrations
{
	public class DataBossExternalCommandMigration : IDataBossMigration
	{
		readonly Func<TextReader> getReader;
 
		public DataBossExternalCommandMigration(string path, Func<TextReader> getReader, DataBossMigrationInfo info) {
			this.Path = path;
			this.getReader = getReader;
			this.Info = info;
		}

		public DataBossMigrationInfo Info { get; }
		public string Path { get;}

		public bool HasQueryBatches => true;

		public IEnumerable<DataBossQueryBatch> GetQueryBatches() => 
			getReader.Select(x => DataBossQueryBatch.ExternalCommand(x, Path));

		IEnumerable<IDataBossMigration> IDataBossMigration.GetSubMigrations() => 
			Enumerable.Empty<IDataBossMigration>();
	}

	public class DataBossQueryMigration : IDataBossMigration
	{
		static readonly Regex BatchSeparatorEx = new Regex(@"(?:\s*go\s*$)", RegexOptions.IgnoreCase | RegexOptions.Compiled);

		readonly Func<TextReader> getReader;
 
		public DataBossQueryMigration(string path, Func<TextReader> getReader, DataBossMigrationInfo info) {
			this.Path = path;
			this.getReader = getReader;
			this.Info = info;
		}

		public DataBossMigrationInfo Info { get; }
		public string Path { get; }

		public bool HasQueryBatches => true;

		public IEnumerable<DataBossQueryBatch> GetQueryBatches() {
			var batch = new StringBuilder();
			Action<string> append = x => (batch.Length == 0 ? batch : batch.AppendLine()).Append(x);
			
			foreach(var line in getReader.AsEnumerable()) {
				var m = BatchSeparatorEx.Match(line);
				if(m.Success) {
					if(m.Index > 0)
						append(line.Substring(0, m.Index));
					if(batch.Length > 0) {
						yield return DataBossQueryBatch.Query(batch.ToString(), Path);
						batch.Clear();
					}
				} else {
					append(line);
				}
			}

			if(batch.Length > 0)
				yield return DataBossQueryBatch.Query(batch.ToString(), Path);
		}

		IEnumerable<IDataBossMigration> IDataBossMigration.GetSubMigrations() => 
			Enumerable.Empty<IDataBossMigration>();
	}
}
