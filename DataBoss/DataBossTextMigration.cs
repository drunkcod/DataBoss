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
		static readonly Regex BatchEx = new Regex(@"(?:\s*go\s*$)", RegexOptions.IgnoreCase | RegexOptions.Compiled);

		readonly Func<TextReader> getReader;
 
		public DataBossTextMigration(Func<TextReader> getReader) {
			this.getReader = getReader;
		}

		public DataBossMigrationInfo Info { get; set; }

		public IEnumerable<string> GetQueryBatches() {
			var batch = new StringBuilder();
			Action<string> append = x => (batch.Length == 0 ? batch : batch.AppendLine()).Append(x);
			
			using(var reader = getReader()) {
				for(string line; (line = reader.ReadLine()) != null;) {
					var m = BatchEx.Match(line);
					if(m.Success) {
						if(m.Index > 0)
							append(line.Substring(0, m.Index));
						if(batch.Length > 0) {
							yield return batch.ToString();
							batch.Clear();
						}
					} else {
						append(line);
					}
				}
			}
			if(batch.Length > 0)
				yield return batch.ToString();
		}
	}
}
