using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Text.RegularExpressions;

namespace DataBoss.Migrations
{
	public class DataBossExternalCommandMigration : IDataBossMigration
	{
		readonly Func<TextReader> getReader;
 
		public DataBossExternalCommandMigration(Func<TextReader> getReader, DataBossMigrationInfo info) {
			this.getReader = getReader;
			this.Info = info;
		}

		public DataBossMigrationInfo Info { get; }

		public bool HasQueryBatches => true;

		public IEnumerable<DataBossQueryBatch> GetQueryBatches() {
			using(var reader = getReader()) {
				for(string line; (line = reader.ReadLine()) != null;) {
					yield return DataBossQueryBatch.ExternalCommand(line);
				}
			}
		}

		IEnumerable<IDataBossMigration> IDataBossMigration.GetSubMigrations() {
			yield break;
		}
	}

	public class DataBossQueryMigration : IDataBossMigration
	{
		static readonly Regex BatchEx = new Regex(@"(?:\s*go\s*$)", RegexOptions.IgnoreCase | RegexOptions.Compiled);

		readonly Func<TextReader> getReader;
 
		public DataBossQueryMigration(Func<TextReader> getReader, DataBossMigrationInfo info) {
			this.getReader = getReader;
			this.Info = info;
		}

		public DataBossMigrationInfo Info { get; }

		public bool HasQueryBatches => true;

		public IEnumerable<DataBossQueryBatch> GetQueryBatches() {
			var batch = new StringBuilder();
			Action<string> append = x => (batch.Length == 0 ? batch : batch.AppendLine()).Append(x);
			
			using(var reader = getReader()) {
				for(string line; (line = reader.ReadLine()) != null;) {
					var m = BatchEx.Match(line);
					if(m.Success) {
						if(m.Index > 0)
							append(line.Substring(0, m.Index));
						if(batch.Length > 0) {
							yield return DataBossQueryBatch.Query(batch.ToString());
							batch.Clear();
						}
					} else {
						append(line);
					}
				}
			}
			if(batch.Length > 0)
				yield return DataBossQueryBatch.Query(batch.ToString());
		}

		IEnumerable<IDataBossMigration> IDataBossMigration.GetSubMigrations() {
			yield break;
		}
	}
}
