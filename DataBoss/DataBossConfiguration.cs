using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Xml.Serialization;
using DataBoss.Specs;

namespace DataBoss
{
	[XmlRoot("db")]
	public class DataBossConfiguration
	{
		[XmlAttribute("server")]
		public string ServerInstance;

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
			var parsedArgs = PowerArgs.Parse(args);
			var config = GetBaseConfig(parsedArgs, load);
			var command = parsedArgs.Commands.SingleOrDefault();
			if(command == null) 
				throw new ArgumentException("missing command and/or configuration options.");

			parsedArgs.Into(config);

			return new KeyValuePair<string,DataBossConfiguration>(command, config);
		}

		private static DataBossConfiguration GetBaseConfig(PowerArgs parsedArgs, Func<string, DataBossConfiguration> load) {
			string target;
			if(parsedArgs.TryGetArg("Target", out target))
				return load(target);

			var targets = Directory.GetFiles(".", "*.databoss");
			if(targets.Length != 1)
				throw new ArgumentException("Can't autodetec target, use -Target <file> to specify it");
			return load(targets[0]);
		}

		public string GetConnectionString() {
			if(string.IsNullOrEmpty(Database))
				throw new InvalidOperationException("No database specified");
			return string.Format("Server={0};Database={1};{2}", ServerInstance ?? ".", Database, GetCredentials());
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
