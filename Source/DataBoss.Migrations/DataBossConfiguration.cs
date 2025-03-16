using DataBoss.Linq;
using DataBoss.Migrations;
using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
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
		public string Server {
			get {
				if (string.IsNullOrEmpty(ServerInstance))
					return IsMsSql ? "." : "localhost";
				return IsMsSql ? ToMsSqlInstance(ServerInstance) : ServerInstance;
			}
		}

		static string ToMsSqlInstance(string instance) {
			var parts = instance.Split(':');
			var host = parts[0] switch {
				"0.0.0.0" => ".",
				_ => parts[0],
			};

			return parts.Length == 1 ? host : $"{host},{parts[1]}";
		}

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
			if (string.IsNullOrEmpty(Database))
				throw new InvalidOperationException("No database specified");

			if (IsMsSql) return AsConnectionString(AddCredentials(new[] {
				("Application Name", "DataBoss"),
				("Pooling", "no"),
				("Data Source", Server ?? "."),
				("Database", Database),
			}));
			if (IsPostgres) return AsConnectionString(
				("Host", ServerInstance ?? "127.0.0.1"),
				("Username", User),
				("Password", Password),
				("Database", Database));

			throw new NotSupportedException();
		}

		IEnumerable<(string, string)> AddCredentials(IEnumerable<(string, string)> xs) {
			if (UseIntegratedSecurity)
				return xs.Concat(new[] { ("Integrated Security", "True") });
			else if (string.IsNullOrEmpty(Password))
				throw new ArgumentException("No Password given for user '" + User + "'");
			else return xs.Concat(new[]{
				("User ID", User),
				("Password", Password)
			});
		}

		static string AsConnectionString(params (string Key, string Value)[] xs) => AsConnectionString(xs.AsEnumerable());
		static string AsConnectionString(IEnumerable<(string Key, string Value)> xs) {
			using var i = xs.GetEnumerator();
			if (!i.MoveNext())
				return string.Empty;
			var sb = new StringBuilder();
			sb.Append(i.Current.Key).Append('=').Append(i.Current.Value);
			while (i.MoveNext())
				sb.Append(';').Append(i.Current.Key).Append('=').Append(i.Current.Value);
			return sb.ToString();
		}

		bool IsMsSql => string.IsNullOrEmpty(Driver) || Driver == "mssql";
		bool IsPostgres => Driver == "postgres";

		public IDbConnection GetDbConnection() {
			var cs = GetConnectionString();
			return IsMsSql ? NewConnection("Microsoft.Data.SqlClient.SqlConnection, Microsoft.Data.SqlClient", cs)
			: IsPostgres ? NewConnection("Npgsql.NpgsqlConnection, Npgsql", cs)
			: throw new NotSupportedException();
		}

		static IDbConnection NewConnection(string typename, string connectionStrinng) {
			var t = Type.GetType(typename) ?? throw new NotSupportedException("Failed to load type " + typename);
			var ctor = t.GetConstructor(new[] { typeof(string) });
			return (IDbConnection)ctor.Invoke(new[] { connectionStrinng });
		}
	}
}
