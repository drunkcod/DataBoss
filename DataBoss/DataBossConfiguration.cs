using System;
using System.Collections.Generic;
using System.IO;
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

		[XmlAttribute("user")]
		public string User;

		[XmlAttribute("password")]
		public string Password;

		[XmlElement("migrations")]
		public DataBossMigrationPath[] Migrations;

		[XmlIgnore]
		public string Script;

		public static DataBossConfiguration Load(string path) {
			var target = path.EndsWith(".databoss") 
			? path
			: path + ".databoss";
			var xml = new XmlSerializer(typeof(DataBossConfiguration));
			using(var input = File.OpenRead(target))
				return (DataBossConfiguration)xml.Deserialize(input);
		}

		public static KeyValuePair<string, DataBossConfiguration> ParseCommandConfig(IEnumerable<string> args) {
			return ParseCommandConfig(args, Load);
		}

		public static KeyValuePair<string, DataBossConfiguration> ParseCommandConfig(IEnumerable<string> args, Func<string, DataBossConfiguration> load) {
			string command = null;
			DataBossConfiguration config = null;
			DataBossConfiguration overrides = null;
			Func<DataBossConfiguration> getOverrides = () => overrides ?? (overrides = new DataBossConfiguration());

			for(var it = args.GetEnumerator(); it.MoveNext();) {
				var item = it.Current;
				if(item.StartsWith("-")) {
					item = item.Substring(1);
					if(!it.MoveNext() || it.Current.StartsWith("-"))
						throw new InvalidOperationException("No value given for '" + item + "'");
					var value = it.Current;
					switch(item) {
						case "User":
							getOverrides().User = value;
							break;
						case "Password": 
							getOverrides().Password = value;
							break;
						case "ServerInstance": 
							getOverrides().Server = value;
							break;
						case "Script":
							getOverrides().Script = value;
							break;
						case "Target":
							config = load(value);
							break;
						default: throw new ArgumentException("Invalid option: " + item);
					}
				}
				else {
					if(command == null)
						command = item;
					else
						throw new ArgumentException("unknown arg: " + item);
				}
			}

			if(config == null) {
				var targets = Directory.GetFiles(".", "*.databoss");
				if(targets.Length != 1)
					throw new ArgumentException("Can't autodetec target, use -Target <file> to specify it");
				config = Load(targets[0]);
			}
			if(config != null && overrides != null) {
				if(!string.IsNullOrEmpty(overrides.Server))
					config.Server = overrides.Server;
				if(!string.IsNullOrEmpty(overrides.Script))
					config.Script = overrides.Script;
				if(!string.IsNullOrEmpty(overrides.User))
					config.User = overrides.User;
				if(!string.IsNullOrEmpty(overrides.Password))
					config.Password = overrides.Password;
			}

			if(command == null || config == null) 
				throw new ArgumentException("missing command and/or configuration options.");

			return new KeyValuePair<string,DataBossConfiguration>(command, config);
		}

		public string GetConnectionString() {
			if(string.IsNullOrEmpty(Database))
				throw new InvalidOperationException("No database specified");
			return string.Format("Server={0};Database={1};{2}", Server ?? ".", Database, GetCredentials());
		}

		public string GetCredentials() {
			if(string.IsNullOrEmpty(User))
				return "Integrated Security=SSPI";
			if(string.IsNullOrEmpty(Password))
				throw new ArgumentException("No Password given for user '" + User + "'");
			return string.Format("User={0};Password={1}", User, Password);
		}
	}
}
