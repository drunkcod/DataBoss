using System.IO;
using Cone;
using DataBoss.Migrations;
using Xunit;

namespace DataBoss.Specs
{
	public class DataBossMigrationPathSpec
	{
		[Fact]
		public void platformizes_paths() => Check.That(
			() => new DataBossMigrationPath { Path = @"Path\SubPath" }.GetOsPath() == Path.Combine("Path", "SubPath"),
			() => new DataBossMigrationPath { Path = @"Path/SubPath" }.GetOsPath() == Path.Combine("Path", "SubPath"),
			//rooted paths stay rooted
			() => new DataBossMigrationPath { Path = @"/Path/SubPath" }.GetOsPath() == Path.Combine(new string(Path.DirectorySeparatorChar, 1), "Path", "SubPath"));
	}
}
