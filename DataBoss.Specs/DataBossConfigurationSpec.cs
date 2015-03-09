using Cone;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

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
						Server = ".",
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
			CommandConfig = DataBossConfiguration.ParseCommandConfig(new [] {
				"-ServerInstance", "MyServer",
				"<command>"
			});

			Check.That(() => CommandConfig.Value.Server == "MyServer");
		}

		public void supports_specifying_Output_script_name() {
			CommandConfig = DataBossConfiguration.ParseCommandConfig(new [] {
				"-Output", "update.sql",
				"<command>"
			});

			Check.That(() => CommandConfig.Value.Script == "update.sql");

		}

		public void raises_InvalidOperationException_for_missing_argument() {
			var ex = Check.Exception<InvalidOperationException>(() => DataBossConfiguration.ParseCommandConfig(new [] {
				"-ServerInstance", 
			}));

			Check.That(() => ex.Message == "No value given for 'ServerInstance'");
		}

		public void requires_Database_to_be_set_when_getting_connection_string() {
			Check.Exception<InvalidOperationException>(() => 
				new DataBossConfiguration { }.GetConnectionString());
		}
	}
}
