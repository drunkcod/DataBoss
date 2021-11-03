using System.IO;
using System.Linq;
using CheckThat;
using DataBoss.Migrations;
using Xunit;

namespace DataBoss
{
	public class DataBossQueryMigrationSpec
	{
		[Theory]
		[InlineData("GO", 0)]
		[InlineData("select 1", 1)]
		[InlineData("select 1 go", 1)]
		[InlineData("select 1 go\\nselect 2", 2)]
		[InlineData("select 1\\nGO\\nselect 2", 2)]
		public void supports_GO_as_batch_separator(string input, int batchCount) {
			input = input.Replace("\\n", "\n");
			Check.That(() => new DataBossQueryMigration(string.Empty, () => new StringReader(input), new DataBossMigrationInfo(), false).GetQueryBatches().Count() == batchCount);
		}

		[Fact]
		public void doesnt_add_extra_newlines() {
			Check.That(() => new DataBossQueryMigration(string.Empty, () => new StringReader("select 42\nGO"), new DataBossMigrationInfo(), false).GetQueryBatches().Single().ToString() == "select 42");
		}
	}
}
