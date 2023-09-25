using DataBoss.Linq;
using DataBoss.Migrations;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Xml.Serialization;

namespace DataBoss
{
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

		[XmlAttribute("defaultSchema")]
		public string DefaultSchema { get; set; }

		[XmlIgnore]
		public string Script { get; set; }

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
				},
				x.IsRepeatable);

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

			using var input = File.OpenRead(target);
			return Load(Path.GetDirectoryName(Path.GetFullPath(target)), input);
		}

		public static DataBossConfiguration Load(string roothPath, Stream input) {
			var xml = new XmlSerializer(typeof(DataBossConfiguration));
			var config = (DataBossConfiguration)xml.Deserialize(input);

			config.Migrations = config.Migrations.ConvertAll(x => x.WithRootPath(roothPath));
			return config;
		}

		public string GetConnectionString() {
			if(string.IsNullOrEmpty(Database))
				throw new InvalidOperationException("No database specified");
			var cs = new SqlConnectionStringBuilder {
				Pooling = false,
				ApplicationName = "DataBoss",
				DataSource = ServerInstance ?? ".",
				InitialCatalog = Database,
			};
			AddCredentials(cs);
			return cs.ToString();
		}

		void AddCredentials(SqlConnectionStringBuilder cs) {
			if(UseIntegratedSecurity)
				cs.IntegratedSecurity = true;
			else if(string.IsNullOrEmpty(Password))
				throw new ArgumentException("No Password given for user '" + User + "'");
			else {
				cs.UserID = User;
				cs.Password = Password;
			}
		}

		public IDbConnection GetDbConnection() => new SqlConnection(GetConnectionString());
	}
}
