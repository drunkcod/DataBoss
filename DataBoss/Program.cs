using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
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

	public class DataBossMigrationInfo
	{
		public long Id;
		public string Context;
		public string Name;

		public string FullId { get { 
			return string.IsNullOrEmpty(Context) ? Id.ToString() : string.Format("{0}.{1}", Context, Id); 
		} }
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

			try {
				var cc = DataBossConfiguration.ParseCommandConfig(args);
				if(!commands.ContainsKey(cc.Key)) {
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

			} catch(Exception e) {
				var oldColor = Console.ForegroundColor;
				Console.ForegroundColor = ConsoleColor.Red;
				Console.Error.WriteLine(e.Message);
				Console.ForegroundColor = oldColor;
				Console.Error.WriteLine();
				Console.Error.WriteLine(ReadResource("Usage").Replace("{{ProgramName}}", ProgramName));
				return -1;
			}

			return 0;
		}

		void Initialize(DataBossConfiguration config) {
			using(var cmd = new SqlCommand(@"
if not exists(select * from sys.tables t where t.name = '__DataBossHistory') begin
	create table __DataBossHistory(
		Id bigint not null,
		Context varchar(64) not null,
		Name varchar(max) not null,
		StartedAt datetime not null,
		FinishedAt datetime,
		[User] varchar(max),
	)

	create clustered index IX_DataBossHistory_StartedAt on __DataBossHistory(StartedAt)

	alter table __DataBossHistory
	add constraint PK_DataBossHistory primary key(
		Id asc,
		Context
	)
end
", db))
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
					Console.WriteLine("  {0} - {1}", item.Info.FullId, item.Info.Name);
			}
		}

		void Update(DataBossConfiguration config) {
			var pending = GetPendingMigrations(config);
			Console.WriteLine("{0} pending migrations found.", pending.Count);

			using(var targetScope = GetTargetScope(config)) {
				var migrator = new DataBossMigrator(info => targetScope);
				pending.ForEach(migrator.Apply);
			}
		}

		IDataBossMigrationScope GetTargetScope(DataBossConfiguration config) {
			if(string.IsNullOrEmpty(config.Script))
				return new DataBossConsoleLogMigrationScope(
					new DataBossSqlMigrationScope(db));
			if(config.Script == "con:")
				return new DataBossScriptMigrationScope(Console.Out, false);
			return new DataBossScriptMigrationScope(new StreamWriter(File.Create(config.Script)), true);
		}

		private List<IDataBossMigration> GetPendingMigrations(DataBossConfiguration config) {
			var applied = new HashSet<string>(GetAppliedMigrations(config).Select(x => x.FullId));
			var migrations = new Queue<IDataBossMigration>();
			var pending = new List<IDataBossMigration>();

			migrations.Enqueue(GetTargetMigration(config.Migrations));
			while(migrations.Count != 0) {
				var item = migrations.Dequeue();
				foreach(var sub in item.GetSubMigrations())
					migrations.Enqueue(sub);
				if(item.HasQueryBatches && !applied.Contains(item.Info.FullId))
					pending.Add(item);
			}
			return pending;
		}

		public static IDataBossMigration GetTargetMigration(DataBossMigrationPath[] migrations) {
			return new DataBossCompositeMigration(
				migrations.ConvertAll(x => new DataBossDirectoryMigration(
					x.Path, new DataBossMigrationInfo {
						Id = 0,
						Context = x.Context,
						Name = x.Path,
					}
				)));
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
