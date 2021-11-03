using CheckThat;
using Xunit;

namespace DataBoss.Migrations
{
	public class DataBossMigrationInfo_
	{
		[Fact]
		public void default_Context_is_empty_string() => Check.That(
			() => new DataBossMigrationInfo().Context == string.Empty);
	}
}
