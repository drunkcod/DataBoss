using System.IO;
using Cone;
using DataBoss.Migrations;

namespace DataBoss.Specs
{
	[Describe(typeof(DataBossMigrationPath))]
	public class DataBossMigrationPathSpec
	{
		public void platformizes_paths() => Check.That(
			() => new DataBossMigrationPath { Path = @"Path\SubPath" }.GetOsPath() == Path.Combine("Path", "SubPath"),
			() => new DataBossMigrationPath { Path = @"Path/SubPath" }.GetOsPath() == Path.Combine("Path", "SubPath"),
			//rooted paths stay rooted
			() => new DataBossMigrationPath { Path = @"/Path/SubPath" }.GetOsPath() == Path.Combine(new string(Path.DirectorySeparatorChar, 1), "Path", "SubPath"));
	}
}
