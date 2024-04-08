using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text;
using CheckThat;
using DataBoss.Migrations;
using Xunit;

namespace DataBoss
{
	public class DataBossConfigurationSpec
	{
		public class when_creating_a_connection_string_assumes
		{
			[Fact]
			public void integrated_security() {
				Check.That(() => 
					new DataBossConfiguration {
						ServerInstance = ".",
						Database = "MyDB"
					}.GetConnectionString().Contains("Integrated Security=True"));
			}

			[Fact]
			public void local_default_instance() {
				var cs = new DataBossConfiguration { 
					Database = "MyDB", 
				}.GetConnectionString();
				
				Check.That(() => cs.Contains("Data Source=."));
			}
		}

		[Fact]
		public void supports_specifying_ServerInstance_as_argument() {
			var config = ParseGivenTargetAndCommand(
				"-ServerInstance", "MyServer"
			);

			Check.That(() => config.Value.ServerInstance == "MyServer");
		}

		[Fact]
		public void supports_specifying_Output_script_name() {
			var config = ParseGivenTargetAndCommand(
				"-Script", "update.sql"
			);

			Check.That(() => config.Value.Script == "update.sql");
		}

		[Fact]
		public void raises_InvalidOperationException_for_missing_argument() {
			var ex = Check.Exception<InvalidOperationException>(() => ParseGivenTargetAndCommand(
				"-ServerInstance"
			));

			Check.That(() => ex.Message == "No value given for 'ServerInstance'");
		}

		[Fact]
		public void uses_supplied_user_and_password_if_available() => Check
			.With(() => new DataBossConfiguration{ Database = ".", User = "sa", Password = "pass" }.GetConnectionString())
			.That(
				cs => cs.Contains("User ID=sa"),
				cs => cs.Contains("Password=pass"));

		[Fact]
		public void requires_Database_to_be_set_when_getting_connection_string() {
			Check.Exception<InvalidOperationException>(() => 
				new DataBossConfiguration { }.GetConnectionString());
		}

		[Fact]
		public void GetCredentials_requires_password_when_user_given() {
			Check.Exception<ArgumentException>(() => new DataBossConfiguration { Database = ".", User = "sa" }.GetConnectionString());
		}

		[Fact]
		public void Migrations_have_absolute_paths() {
			var config = DataBossConfiguration.Load(Path.Combine("X:", "Project"), StringStream("<db><migrations path=\"Migrations\"/></db>"));
			Check.That(() => config.Migrations[0].Path == Path.Combine("X:", "Project", "Migrations"));
		}

		[Fact]
		public void can_be_created_from_SqlConnectionStringBuilder() {
			Check.With(() => Create(new SqlConnectionStringBuilder("Server=TheServer;Initial Catalog=TheDatabase")))
				.That(
					x => x.ServerInstance == "TheServer",
					x => x.Database == "TheDatabase",
					x => x.UseIntegratedSecurity);				
		}

		[Fact]
		public void can_specify_migrations_when_created_from_connection_string() {
			var cs = new SqlConnectionStringBuilder("Server=TheServer;Initial Catalog=TheDatabase");
			var migrations = Array.Empty<DataBossMigrationPath>();
			Check.With(() => Create(cs, migrations))
				.That(x => x.Migrations == migrations);
		}

		static Stream StringStream(string data) =>
			new MemoryStream(Encoding.UTF8.GetBytes(data));

		static KeyValuePair<string, DataBossConfiguration> ParseGivenTargetAndCommand(params string[] args) {
			return global::DataBoss.Program.ParseCommandConfig(
				args.Concat(new[] {
					"-Target", "target",
					"<command>"
				}), 
				_ => new DataBossConfiguration());
		}

		public static DataBossConfiguration Create(SqlConnectionStringBuilder connectionString, params DataBossMigrationPath[] migrationPaths) {
			return new DataBossConfiguration {
				Database = connectionString.InitialCatalog,
				ServerInstance = connectionString.DataSource,
				Migrations = migrationPaths,
			};
		}

	}
}
