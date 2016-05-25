using Cone;
using System.IO;
using System.Linq;
using DataBoss.Migrations;

namespace DataBoss.Specs
{
	[Describe(typeof(DataBossQueryMigration))]
	public class DataBossQueryMigrationSpec
	{
		[DisplayAs("'{0}' has {1} batches", Heading = "supports GO as batch separator")
		,Row("GO", 0)
		,Row("select 1", 1)
		,Row("select 1 go", 1)
		,Row("select 1 go\\nselect 2", 2)
		,Row("select 1\\nGO\\nselect 2", 2)]
		public void supports_GO_as_batch_separator(string input, int batchCount) {
			input = input.Replace("\\n", "\n");
			Check.That(() => new DataBossQueryMigration(() => new StringReader(input)).GetQueryBatches().Count() == batchCount);
		}

		public void doesnt_add_extra_newlines() {
			Check.That(() => new DataBossQueryMigration(() => new StringReader("select 42\nGO")).GetQueryBatches().Single().ToString() == "select 42");
		}
	}
}
