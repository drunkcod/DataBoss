using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using System.Xml.Serialization;

namespace DataBoss
{
	public class DataBossMigrationPath
	{
		[XmlAttribute("context")]
		public string Context;

		[XmlAttribute("path")]
		public string Path;
	}

	public interface IDataBossMigration
	{
		DataBossMigrationInfo Info { get; }
		IEnumerable<string> GetQueryBatches();
	}

	public class DataBossLocalMigration : IDataBossMigration
	{
		public DataBossMigrationInfo Info { get; set; }
		public string Path;

		public IEnumerable<string> GetQueryBatches() {
			yield return File.ReadAllText(Path);
		}
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

		readonly SqlConnection db;

		static string ReadResource(string path) {
			using(var reader = new StreamReader(typeof(Program).Assembly.GetManifestResourceStream(path)))
				return reader.ReadToEnd();
		}

		public Program(SqlConnection db) {
			this.db = db;
		}

		static int Main(string[] args) {
			var commands = new Dictionary<string, Action<Program, DataBossConfiguration>> {
				{ "init", (p, c) => p.Initialize(c) },
				{ "status", (p, c) => p.Status(c) },
				{ "update", (p, c) => p.Update(c) },
			};

			KeyValuePair<string, DataBossConfiguration> cc;

			try {
				if(!DataBossConfiguration.TryParseCommandConfig(args, out cc) || !commands.ContainsKey(cc.Key)) {
					Console.WriteLine(ReadResource("Usage").Replace("{{ProgramName}}", ProgramName));
					return -1;
				}
			
				var command = cc.Key;
				var config = cc.Value;

				using(var db = new SqlConnection(config.GetConnectionString())) {
					db.Open();
					var program = new Program(db);
					commands[command](program, config);
				}

			} catch(InvalidOperationException e) {
				Console.Error.WriteLine(e.Message);
				return -1;
			}

			return 0;
		}

		void Initialize(DataBossConfiguration config) {
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
				using(var r = cmd.ExecuteReader())
					while(r.Read())
						Console.WriteLine(r.GetValue(0));
			}
		}
	
		void Status(DataBossConfiguration config) {
			var pending = GetPendingMigrations(config);
			if(pending.Count != 0) {
				Console.WriteLine("Pending migrations:");
				foreach(var item in pending)
					Console.WriteLine("  {0}. {1}", item.Info.Id, item.Info.Name);
			}
		}

		void Update(DataBossConfiguration config) {
			var pending = GetPendingMigrations(config);
			Console.WriteLine("{0} pending migrations found.", pending.Count);

			var migrator = new DataBossMigrator(info => new DataBossConsoleLogMigrationScope(new DataBossSqlMigrationScope(db)));
			pending.ForEach(migrator.Apply);
		}

		private List<DataBossLocalMigration> GetPendingMigrations(DataBossConfiguration config) {
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

		public IEnumerable<DataBossMigrationInfo> GetAppliedMigrations(DataBossConfiguration config) {
			using(var cmd = new SqlCommand("select count(*) from sys.tables where name = '__DataBossHistory'", db)) {
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
