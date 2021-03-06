using DataBoss.Linq;
using DataBoss.Migrations;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Xml.Serialization;

namespace DataBoss
{
	public interface IDataBossConfiguration
	{
		string GetConnectionString();
		IDataBossMigration GetTargetMigration();
	}

	[XmlRoot("db")]
	public class DataBossConfiguration : IDataBossConfiguration
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

		[XmlIgnore]
		public bool UseIntegratedSecurity => string.IsNullOrEmpty(User);

		public IDataBossMigration GetTargetMigration() =>
			new DataBossCompositeMigration(Migrations.ConvertAll(MakeDirectoryMigration));

		static IDataBossMigration MakeDirectoryMigration(DataBossMigrationPath x) => 
			new DataBossDirectoryMigration(
				x.Path, 
				new DataBossMigrationInfo {
					Id = 0,
					Context = x.Context,
					Name = x.Path,
				});

		public static DataBossConfiguration Create(SqlConnectionStringBuilder connectionString, params DataBossMigrationPath[] migrationPaths) {
			return new DataBossConfiguration {
				Database = connectionString.InitialCatalog,
				ServerInstance = connectionString.DataSource,
				Migrations = migrationPaths,
			};
		}

		public static DataBossConfiguration Load(string path) {
			var target = path.EndsWith(".databoss") 
			? path
			: path + ".databoss";
			using (var input = File.OpenRead(target))
				return Load(Path.GetDirectoryName(Path.GetFullPath(target)), input);
		}

		public static DataBossConfiguration Load(string roothPath, Stream input) {
			var xml = new XmlSerializer(typeof(DataBossConfiguration));
			var config = (DataBossConfiguration)xml.Deserialize(input);

			config.Migrations = config.Migrations.ConvertAll(x => new DataBossMigrationPath {
				Context = x.Context,
				Path = Path.Combine(roothPath, x.GetOsPath())
			});
			return config;
		}

		public static KeyValuePair<string, DataBossConfiguration> ParseCommandConfig(IEnumerable<string> args) => ParseCommandConfig(args, Load);

		public static KeyValuePair<string, DataBossConfiguration> ParseCommandConfig(IEnumerable<string> args, Func<string, DataBossConfiguration> load) {
			var parsedArgs = PowerArgs.Parse(args);
			var config = GetBaseConfig(parsedArgs, load);
			var command = parsedArgs.Commands.SingleOrDefault() ?? throw new ArgumentException("missing command and/or configuration options.");

			parsedArgs.Into(config);

			return new KeyValuePair<string,DataBossConfiguration>(command, config);
		}

		private static DataBossConfiguration GetBaseConfig(PowerArgs parsedArgs, Func<string, DataBossConfiguration> load) {
			if(parsedArgs.TryGetArg("Target", out var target))
				return load(target);

			var targets = Directory.GetFiles(".", "*.databoss");
			if(targets.Length != 1)
				throw new ArgumentException("Can't autodetect target, use -Target <file> to specify it");
			return load(targets[0]);
		}

		public string GetConnectionString() {
			if(string.IsNullOrEmpty(Database))
				throw new InvalidOperationException("No database specified");
			return $"Server={ServerInstance ?? "."};Database={Database};{GetCredentials()}";
		}

		public string GetCredentials() {
			if(UseIntegratedSecurity)
				return "Integrated Security=SSPI";
			if(string.IsNullOrEmpty(Password))
				throw new ArgumentException("No Password given for user '" + User + "'");
			return $"User={User};Password={Password}";
		}
	}
}
