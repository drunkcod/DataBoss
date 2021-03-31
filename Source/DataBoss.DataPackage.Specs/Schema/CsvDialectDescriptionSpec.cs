using CheckThat;
using Xunit;

namespace DataBoss.DataPackage.Schema
{
	public class CsvDialectDescriptionSpec
	{
		[Fact]
		public void default_dialiect() => Check
			.With(() => CsvDialectDescription.GetDefaultDialect())
			.That(csv => csv.Delimiter == ",");
	}
}
