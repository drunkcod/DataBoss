using System.Linq;
using CheckThat;
using DataBoss.Migrations;
using Xunit;

namespace DataBoss.Specs
{
	public class DataBossDirectoryMigrationSpec
	{
		[Fact]
		public void excludes_files_extension_from_name() {
			var directory = new DataBossDirectoryMigration("MyDirectory", new DataBossMigrationInfo());
			var migrations = directory.GetMigrations(new [] { "001 First File Migration.sql" }).ToArray();

			Check.That(
				() => migrations.Length == 1,
				() => migrations[0].Key == "001 First File Migration.sql",
				() => migrations[0].Value.Id == 1,
				() => migrations[0].Value.Name == "First File Migration"
			);
		}

		[Fact]
		public void directory_migrations_use_path_minus_id_as_name() {
			var directory = new DataBossDirectoryMigration("MyDirectory", new DataBossMigrationInfo());
			var migrations = directory.GetMigrations(new [] { "001 First" }).ToArray();

			Check.That(() => migrations.Length == 1);
			Check.That(
				() => migrations[0].Key == "001 First",
				() => migrations[0].Value.Id == 1,
				() => migrations[0].Value.Name == "First"
			);
		}
	}
}
