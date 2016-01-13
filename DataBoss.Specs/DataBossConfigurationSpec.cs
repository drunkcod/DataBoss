using Cone;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace DataBoss.Specs
{
	[Describe(typeof(DataBossConfiguration))]
	public class DataBossConfigurationSpec
	{
		KeyValuePair<string, DataBossConfiguration> CommandConfig;

		[Context("when creating a connection string assumes")]
		public class DataBossConfigurationGetConnectionStringAssumesSpec
		{
			public void integrated_security() {
				Check.That(() => 
					new DataBossConfiguration {
						ServerInstance = ".",
						Database = "MyDB"
					}.GetConnectionString() == "Server=.;Database=MyDB;Integrated Security=SSPI");
			}

			public void local_default_instance() {
				Check.That(() =>
					new DataBossConfiguration { 
						Database = "MyDB", 
					}.GetConnectionString().StartsWith("Server=."));
			}
		}

		public void supports_specifying_ServerInstance_as_argument() {
			CommandConfig = ParseGivenTargetAndCommand(
				"-ServerInstance", "MyServer"
			);

			Check.That(() => CommandConfig.Value.ServerInstance == "MyServer");
		}

		public void supports_specifying_Output_script_name() {
			CommandConfig = ParseGivenTargetAndCommand(
				"-Script", "update.sql"
			);

			Check.That(() => CommandConfig.Value.Script == "update.sql");
		}

		public void raises_InvalidOperationException_for_missing_argument() {
			var ex = Check.Exception<InvalidOperationException>(() => ParseGivenTargetAndCommand(
				"-ServerInstance"
			));

			Check.That(() => ex.Message == "No value given for 'ServerInstance'");
		}

		public void uses_supplied_user_and_password_if_available() {
			Check.That(() => new DataBossConfiguration{ Database = ".", User = "sa", Password = "pass" }.GetConnectionString().EndsWith("User=sa;Password=pass"));
		}

		public void requires_Database_to_be_set_when_getting_connection_string() {
			Check.Exception<InvalidOperationException>(() => 
				new DataBossConfiguration { }.GetConnectionString());
		}

		public void GetCredentials_requires_password_when_user_given() {
			Check.Exception<ArgumentException>(() => new DataBossConfiguration { User = "sa" }.GetCredentials());
		}

		public void Migrations_have_absolute_paths() {
			var config = DataBossConfiguration.Load("X:\\Project", StringStream("<db><migrations path=\"Migrations\"/></db>"));
			Check.That(() => config.Migrations[0].Path == "X:\\Project\\Migrations");
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
