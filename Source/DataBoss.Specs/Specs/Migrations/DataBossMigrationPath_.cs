using System.IO;
using CheckThat;
using DataBoss.Migrations;
using Xunit;

namespace DataBoss.Migrations
{
	public class DataBossMigrationPath_
	{
		[Fact]
		public void platformizes_paths() => Check.That(
			() => new DataBossMigrationPath { Path = @"Path\SubPath" }.GetOsPath() == Path.Combine("Path", "SubPath"),
			() => new DataBossMigrationPath { Path = @"Path/SubPath" }.GetOsPath() == Path.Combine("Path", "SubPath"),
			//rooted paths stay rooted
			() => new DataBossMigrationPath { Path = @"/Path/SubPath" }.GetOsPath() == Path.Combine(new string(Path.DirectorySeparatorChar, 1), "Path", "SubPath"));
	}
}
