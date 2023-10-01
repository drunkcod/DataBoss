using DataBoss.Linq;
using DataBoss.Migrations;
using System;
using System.Data;
using System.IO;
using System.Text;
using System.Xml.Serialization;

namespace DataBoss
{
	[XmlRoot("db")]
	public class DataBossConfiguration : IDataBossConfiguration
	{
		[XmlAttribute("driver")]
		public string Driver;

		[XmlAttribute("server")]
		public string ServerInstance;

		[XmlAttribute("database")]
		public string Database { get; set; }

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

		[XmlIgnore]
		public string Server => ServerInstance ?? "localhost";

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

			if(IsMsSql) {
				var cs = new StringBuilder()
					.Append("Application Name=DataBoss;Pooling=no")
					.Append("Data Source=").Append(ServerInstance ?? ".").Append(';')
					.Append("Database=").Append(Database).Append(';');
				AddCredentials(cs);
				return cs.ToString();
			}
			if(IsPostgres) {
				return $"Host={ServerInstance ?? "127.0.0.1"};Username={User};Password={Password};Database={Database}";
			}
			throw new NotSupportedException();
		}

		void AddCredentials(StringBuilder cs) {
			if(UseIntegratedSecurity)
				cs.Append("Integrated Security=True;");
			else if(string.IsNullOrEmpty(Password))
				throw new ArgumentException("No Password given for user '" + User + "'");
			else cs
				.Append("User ID=").Append(User)
				.Append(";Password=").Append(Password).Append(';');
		}

		bool IsMsSql => string.IsNullOrEmpty(Driver) || Driver == "mssql";
		bool IsPostgres => Driver == "postgres";

		public IDbConnection GetDbConnection() {
			var cs = GetConnectionString();
			return IsMsSql ? NewConnection("System.Data.SqlClient, System.Data.SqlClient.SqlConnection", cs)
			: IsPostgres ? NewConnection("Npgsql, Npgsql.NpgSqlConnection", cs)
			: throw new NotSupportedException();
		}

		static IDbConnection NewConnection(string typename, string connectionStrinng) {
			var t = Type.GetType(typename) ?? throw new NotSupportedException("Failed to load type " + typename);
			var ctor = t.GetConstructor(new[]{ typeof(string) });
			return (IDbConnection)ctor.Invoke(new[]{ connectionStrinng });
		}
	}
}
