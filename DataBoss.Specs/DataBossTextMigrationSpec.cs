using Cone;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataBoss.Specs
{
	[Describe(typeof(DataBossTextMigration))]
	public class DataBossTextMigrationSpec
	{
		[DisplayAs("'{0}' has {1} batches", Heading = "supports GO as batch separator")
		,Row("select 1", 1)
		,Row("select 1 go", 1)
		,Row("select 1 go\\nselect 2", 2)
		,Row("select 1\\nGO\\nselect 2", 2)]
		public void supports_GO_as_batch_separator(string input, int batchCount) {
			input = input.Replace("\\n", "\n");
			Check.That(() => new DataBossTextMigration(() => new StringReader(input)).GetQueryBatches().Count() == batchCount);
		}
	}
}
