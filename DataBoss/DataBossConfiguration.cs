using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Serialization;

namespace DataBoss
{
	[XmlRoot("db")]
	public class DataBossConfiguration
	{
		[XmlAttribute("server")]
		public string Server;

		[XmlAttribute("database")]
		public string Database;

		[XmlElement("migrations")]
		public DataBossMigrationPath Migration;

		[XmlElement("Script")]
		public string Script;

		public static DataBossConfiguration Load(string path) {
			var xml = new XmlSerializer(typeof(DataBossConfiguration));
			using(var input = File.OpenRead(path))
				return (DataBossConfiguration)xml.Deserialize(input);
		}

		public static KeyValuePair<string, DataBossConfiguration> ParseCommandConfig(IEnumerable<string> args) {
			string command = null;
			DataBossConfiguration config = null;
			DataBossConfiguration overrides = null;
			Func<DataBossConfiguration> getOverrides = () => overrides ?? (overrides = new DataBossConfiguration());

			for(var it = args.GetEnumerator(); it.MoveNext();) {
				var item = it.Current;
				if(item.StartsWith("-")) {
					switch(item) {
						case "-ServerInstance": 
							if(!it.MoveNext())
								throw new InvalidOperationException("No value given for 'ServerInstance'");
							getOverrides().Server = it.Current;
							break;
						case "-Script":
							if(!it.MoveNext())
								throw new InvalidOperationException("No value given for 'Output'");
							getOverrides().Script = it.Current;
							break;
						default: throw new ArgumentException("Invalid option: " + item);
					}
				}
				else {
					if(command == null)
						command = item;
					else if(config == null) {
						var target = item.EndsWith(".databoss") ? item : item + ".databoss";
						config = DataBossConfiguration.Load(target);
					}
					else {
						throw new ArgumentException("unknown arg: " + item);
					}
				}
			}

			if(config == null)
				config = overrides;
			else if(config != null && overrides != null) {
				if(!string.IsNullOrEmpty(overrides.Server))
					config.Server = overrides.Server;
				if(!string.IsNullOrEmpty(overrides.Script))
					config.Script = overrides.Script;
			}

			if(command == null || config == null) 
				throw new ArgumentException("missing command and/or configuration options.");

			return new KeyValuePair<string,DataBossConfiguration>(command, config);
		}

		public string GetConnectionString() {
			if(string.IsNullOrEmpty(Database))
				throw new InvalidOperationException("No database specified");
			return string.Format("Server={0};Database={1};Integrated Security=SSPI", Server ?? ".", Database);
		}
	}
}
