using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using System.Xml.Serialization;

namespace DataBoss
{
	[XmlRoot("db")]
	public class DataBossConfiguration
	{
		[XmlElement("connectionString")]
		public string ConnectionString;

		[XmlElement("migrations")]
		public DataBossMigrationPath Migration;

		public static DataBossConfiguration Load(string path) {
			var xml = new XmlSerializer(typeof(DataBossConfiguration));
			using(var input = File.OpenRead(path))
				return (DataBossConfiguration)xml.Deserialize(input);
		}
	}

	public class DataBossMigrationPath
	{
		[XmlAttribute("context")]
		public string Context;

		[XmlAttribute("path")]
		public string Path;
	}

	public class DataBossLocalMigration
	{
		public DataBossMigrationInfo Info;
		public string Path;
	}

	public class DataBossMigrationInfo
	{
		public long Id;
		public string Context;
		public string Name;
	}

	public class Program
	{
		static string ProgramName { get { return Path.GetFileName(typeof(Program).Assembly.Location); } }

		static string ReadResource(string path) {
			using(var reader = new StreamReader(typeof(Program).Assembly.GetManifestResourceStream(path)))
				return reader.ReadToEnd();
		}

		static int Main(string[] args) {
			var commands = new Dictionary<string, Action<DataBossConfiguration>>
			{
				{ "init", Initialize },
				{ "status", Status },
				{ "update", Update },
			};

			if(args.Length != 2 || !commands.ContainsKey(args[0])) {
				Console.WriteLine(ReadResource("Usage").Replace("{{ProgramName}}", ProgramName));
				return -1;
			}

			var command = args[0];
			var target = args[1];
			if(!target.EndsWith(".databoss"))
				target = target + ".databoss";

			var config = DataBossConfiguration.Load(target);
			try {
				commands[command](config);
			} catch(InvalidOperationException e) {
				Console.Error.WriteLine(e.Message);
			}

			return 0;
		}

		static void Initialize(DataBossConfiguration config) {
			using(var db = new SqlConnection(config.ConnectionString))
			using(var cmd = new SqlCommand(@"
if not exists(select * from sys.tables t where t.name = '__DataBossHistory')
	create table __DataBossHistory(
		Id bigint not null primary key,
		Context varchar(max) not null,
		Name varchar(max) not null,
		StartedAt datetime not null,
		FinishedAt datetime,
		[User] varchar(max)
	)", db))
			{
				db.Open();
				using(var r = cmd.ExecuteReader())
					while(r.Read())
						Console.WriteLine(r.GetValue(0));
			}
		}
	
		static void Status(DataBossConfiguration config) {
			var pending = GetPendingMigrations(config);
			if(pending.Count != 0) {
				Console.WriteLine("Pending migrations:");
				foreach(var item in pending)
					Console.WriteLine("  {0}. {1}", item.Info.Id, item.Info.Name);
			}
		}

		static void Update(DataBossConfiguration config) {
			var pending = GetPendingMigrations(config);
			Console.WriteLine("{0} pending migrations found.", pending.Count);
			foreach(var item in pending) {
				Console.WriteLine("  Applying: {0}. {1}", item.Info.Id, item.Info.Name);
				using(var db = new SqlConnection(config.ConnectionString))
				using(var cmd = new SqlCommand("insert __DataBossHistory(Id, Context, Name, StartedAt, [User]) values(@id, @context, @name, getdate(), @user)", db)) {
					cmd.Parameters.AddWithValue("@id", item.Info.Id);
					cmd.Parameters.AddWithValue("@context", item.Info.Context ?? string.Empty);
					cmd.Parameters.AddWithValue("@name", item.Info.Name);
					cmd.Parameters.AddWithValue("@user", Environment.UserName);
					
					db.Open();
					cmd.ExecuteNonQuery();
					
					using(var q = new SqlCommand(File.ReadAllText(item.Path), db))
						q.ExecuteNonQuery();

					cmd.CommandText = "update __DataBossHistory set FinishedAt = getdate() where Id = @id";
					cmd.ExecuteNonQuery();
				}
			}
		}

		private static List<DataBossLocalMigration> GetPendingMigrations(DataBossConfiguration config) {
			var applied = new HashSet<long>(GetAppliedMigrations(config).Select(x => x.Id));
			return GetLocalMigrations(config.Migration).Where(x => !applied.Contains(x.Info.Id)).OrderBy(x => x.Info.Id).ToList();
		}

		public static IEnumerable<DataBossLocalMigration> GetLocalMigrations(DataBossMigrationPath migrations) {
			var r = new Regex(@"(?<id>\d+)(?<name>.*).sql$");
			var groups = new {
				id = r.GroupNumberFromName("id"),
				name = r.GroupNumberFromName("name"),
			};
			return 
				Directory.GetFiles(migrations.Path, "*.sql", SearchOption.AllDirectories)
				.ConvertAll(x => new {
					m = r.Match(Path.GetFileName(x)),
					path = x,
				}).Where(x => x.m.Success)
				.Select(x => new DataBossLocalMigration {
					Path = x.path,
					Info = new DataBossMigrationInfo {
						Id = long.Parse(x.m.Groups[groups.id].Value),
						Context = migrations.Context,
						Name = x.m.Groups[groups.name].Value.Trim(),
					}
				});
		}

		public static IEnumerable<DataBossMigrationInfo> GetAppliedMigrations(DataBossConfiguration config) {
			using(var db = new SqlConnection(config.ConnectionString))
			using(var cmd = new SqlCommand("select count(*) from sys.tables where name = '__DataBossHistory'", db)) {
				db.Open();
				if((int)cmd.ExecuteScalar() == 0)
					throw new InvalidOperationException(string.Format("DataBoss has not been initialized, run: {0} init <target>", ProgramName));

				cmd.CommandText = "select Id, Context, Name from __DataBossHistory";
				using(var reader = cmd.ExecuteReader()) {
					var ordinals = new {
						Id = reader.GetOrdinal("Id"),
						Context = reader.GetOrdinal("Context"),
						Name = reader.GetOrdinal("Name"),
					};
					while(reader.Read())
						yield return new DataBossMigrationInfo {
							Id = reader.GetInt64(ordinals.Id),
							Name = reader.GetString(ordinals.Name),
							Context = reader.GetString(ordinals.Context),
						};
				}
			}
		}
	}
}
