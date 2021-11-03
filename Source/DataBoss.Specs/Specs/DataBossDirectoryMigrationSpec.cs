using System.Linq;
using CheckThat;
using DataBoss.Migrations;
using Xunit;

namespace DataBoss
{
	public class DataBossDirectoryMigrationSpec
	{
		[Fact]
		public void excludes_files_extension_from_name() {
			var directory = new DataBossDirectoryMigration("MyDirectory", new DataBossMigrationInfo(), false);
			var migrations = directory.GetMigrationInfo("001 First File Migration.sql");

			Check.That(
				() => migrations.Id == 1,
				() => migrations.Name == "First File Migration"
			);
		}

		[Fact]
		public void directory_migrations_use_path_minus_id_as_name() {
			var directory = new DataBossDirectoryMigration("MyDirectory", new DataBossMigrationInfo(), false);
			var migrations = directory.GetMigrationInfo("001 First");

			Check.That(
				() => migrations.Id == 1,
				() => migrations.Name == "First"
			);
		}
	}
}
