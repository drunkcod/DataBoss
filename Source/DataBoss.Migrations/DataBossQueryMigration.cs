using DataBoss.Linq;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;

namespace DataBoss.Migrations
{
	public class DataBossQueryMigration : IDataBossMigration
	{
		static readonly Regex BatchSeparatorEx = new(@"(?:\s*go\s*$)", RegexOptions.IgnoreCase | RegexOptions.Compiled);

		readonly Func<TextReader> getReader;
 
		public DataBossQueryMigration(string path, Func<TextReader> getReader, DataBossMigrationInfo info, bool isRepeatable) {
			this.Path = path;
			this.getReader = getReader;
			this.Info = info;
			this.IsRepeatable = isRepeatable;
		}

		public DataBossMigrationInfo Info { get; }
		public string Path { get; }
		public bool IsRepeatable { get; }
		public bool HasQueryBatches => true;

		public IEnumerable<DataBossQueryBatch> GetQueryBatches() {
			var batch = new StringBuilder();
			void Append(string x) => (batch.Length == 0 ? batch : batch.AppendLine()).Append(x);
			
			foreach(var line in getReader.AsEnumerable()) {
				var m = BatchSeparatorEx.Match(line);
				if(m.Success) {
					if(m.Index > 0)
						Append(line.Substring(0, m.Index));
					if(batch.Length > 0) {
						yield return DataBossQueryBatch.Query(batch.ToString(), Path);
						batch.Clear();
					}
				} else {
					Append(line);
				}
			}

			if(batch.Length > 0)
				yield return DataBossQueryBatch.Query(batch.ToString(), Path);
		}

		IEnumerable<IDataBossMigration> IDataBossMigration.GetSubMigrations() => 
			Enumerable.Empty<IDataBossMigration>();
	}
}
