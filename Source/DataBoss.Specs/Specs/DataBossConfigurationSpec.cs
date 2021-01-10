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
					}.GetConnectionString() == "Server=.;Database=MyDB;Integrated Security=SSPI");
			}

			[Fact]
			public void local_default_instance() {
				Check.That(() =>
					new DataBossConfiguration { 
						Database = "MyDB", 
					}.GetConnectionString().StartsWith("Server=."));
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
		public void uses_supplied_user_and_password_if_available() {
			Check.That(() => new DataBossConfiguration{ Database = ".", User = "sa", Password = "pass" }.GetConnectionString().EndsWith("User=sa;Password=pass"));
		}

		[Fact]
		public void requires_Database_to_be_set_when_getting_connection_string() {
			Check.Exception<InvalidOperationException>(() => 
				new DataBossConfiguration { }.GetConnectionString());
		}

		[Fact]
		public void GetCredentials_requires_password_when_user_given() {
			Check.Exception<ArgumentException>(() => new DataBossConfiguration { User = "sa" }.GetCredentials());
		}

		[Fact]
		public void Migrations_have_absolute_paths() {
			var config = DataBossConfiguration.Load("X:\\Project", StringStream("<db><migrations path=\"Migrations\"/></db>"));
			Check.That(() => config.Migrations[0].Path == "X:\\Project\\Migrations");
		}

		[Fact]
		public void can_be_created_from_SqlConnectionStringBuilder() {
			Check.With(() => DataBossConfiguration.Create(new SqlConnectionStringBuilder("Server=TheServer;Initial Catalog=TheDatabase")))
				.That(
					x => x.ServerInstance == "TheServer",
					x => x.Database == "TheDatabase",
					x => x.UseIntegratedSecurity);				
		}

		[Fact]
		public void can_specify_migrations_when_created_from_connection_string() {
			var cs = new SqlConnectionStringBuilder("Server=TheServer;Initial Catalog=TheDatabase");
			var migrations = new DataBossMigrationPath[0];
			Check.With(() => DataBossConfiguration.Create(cs, migrations))
				.That(x => x.Migrations == migrations);
		}

		Stream StringStream(string data) {
			return new MemoryStream(Encoding.UTF8.GetBytes(data));
		}

		KeyValuePair<string, DataBossConfiguration> ParseGivenTargetAndCommand(params string[] args) {
			return DataBossConfiguration.ParseCommandConfig(
				args.Concat(new[] {
					"-Target", "target",
					"<command>"
				}), 
				_ => new DataBossConfiguration());
		}
	}
}
