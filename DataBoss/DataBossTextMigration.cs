using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace DataBoss
{
	public class DataBossTextMigration : IDataBossMigration
	{
		readonly Func<TextReader> getReader;
 
		public DataBossTextMigration(Func<TextReader> getReader) {
			this.getReader = getReader;
		}

		public DataBossMigrationInfo Info { get; set; }

		public IEnumerable<string> GetQueryBatches() {
			var r = new Regex(@"(.*?)(?:\s*go\s*$)", RegexOptions.IgnoreCase);
			var batch = new StringBuilder();
			using(var reader = getReader()) {
				for(string line; (line = reader.ReadLine()) != null;) {
					var m = r.Match(line);
					if(m.Success) {
						batch.AppendLine(m.Groups[0].Value);
						yield return batch.ToString();
						batch.Clear();
					} else {
						batch.AppendLine(line);
					}
				}
			}
			if(batch.Length > 0)
				yield return batch.ToString();
		}
	}

}
