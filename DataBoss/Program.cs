using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using DataBoss.Schema;

namespace DataBoss
{
	delegate int DataBossAction(Program program, DataBossConfiguration config);

	public class Program
	{
		static string ProgramName => Path.GetFileName(typeof(Program).Assembly.Location);

		readonly IDataBossLog log;
		readonly SqlConnection db;
		readonly DataBossScripter scripter = new DataBossScripter();
		readonly ObjectReader objectReader = new ObjectReader();

		public Program(IDataBossLog log, SqlConnection db) {
			this.log = log;
			this.db = db;
		}

		static int Main(string[] args) {
			if(args.Length == 0) {
				Console.WriteLine(GetUsageString());
				return 0;
			}

			var log = new DataBossConsoleLog();

			try {
				var cc = DataBossConfiguration.ParseCommandConfig(args);

				DataBossAction command;
				if(!TryGetCommand(cc.Key, out command)) {
					log.Info(GetUsageString());
					return -1;
				}

				using(var db = new SqlConnection(cc.Value.GetConnectionString())) {
					return command(new Program(log, db), cc.Value);
				}

			} catch(Exception e) {
				log.Error(e);
				log.Info(GetUsageString());
				return -1;
			}
		}

		[DataBossCommand("init")]
		public int Initialize(DataBossConfiguration config) {
			EnsureDataBase(config.GetConnectionString());
			using(var cmd = new SqlCommand(scripter.CreateMissing(typeof(DataBossHistory)), db))
			{
				Open();
				using(var r = cmd.ExecuteReader())
					while(r.Read())
						log.Info("{0}", r.GetValue(0));
			}
			return 0;
		}

		[DataBossCommand("status")]
		public int Status(DataBossConfiguration config) {
			Open();
			var pending = GetPendingMigrations(config);
			if(pending.Count != 0) {
				var message = new StringBuilder();
				message.AppendLine("Pending migrations:");
				foreach(var item in pending)
					message.AppendFormat("  {0} - {1}", item.Info.FullId, item.Info.Name);
				log.Info(message.ToString());
			}
			return pending.Count;
		}

		[DataBossCommand("update")]
		public int Update(DataBossConfiguration config) {
			Open();
			var pending = GetPendingMigrations(config);
			log.Info("{0} pending migrations found.", pending.Count);

			using(var targetScope = GetTargetScope(config)) {
				var migrator = new DataBossMigrator(info => targetScope);
				return migrator.ApplyRange(pending) ? 0 : -1;
			}
		}

		static bool TryGetCommand(string name, out DataBossAction command) {
			var target = typeof(Program)
				.GetMethods(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic)
				.Select(x => new {
					Method = x,
					Command = x.SingleOrDefault<DataBossCommandAttribute>()
				})
				.FirstOrDefault(x => x?.Command.Name == name);
			if(target == null)
				command = null;
			else
				command = (DataBossAction)Delegate.CreateDelegate(typeof(DataBossAction), target.Method);
			return command != null;
		}

		static string GetUsageString() {
			return ReadResource("Usage")
				.Replace("{{ProgramName}}", ProgramName)
				.Replace("{{Version}}", typeof(Program).Assembly.GetName().Version.ToString());
		}

		static string ReadResource(string path) {
			using (var reader = new StreamReader(typeof(Program).Assembly.GetManifestResourceStream(path)))
				return reader.ReadToEnd();
		}

		static void EnsureDataBase(string connectionString) {
			var qs = new SqlConnectionStringBuilder(connectionString);
			var dbName = qs.InitialCatalog;
			qs.Remove("Initial Catalog");
			using(var db = new SqlConnection(qs.ConnectionString)) {
				db.Open();
				using(var cmd = new SqlCommand("select db_id(@db)" ,db)) {
					cmd.Parameters.AddWithValue("@db", dbName);
					if(cmd.ExecuteScalar() is DBNull) {
						cmd.CommandText = $"create database [{dbName}]";
						cmd.ExecuteNonQuery();
					}
				}
			}
		}

		void Open() {
			if(db.State != ConnectionState.Open)
				db.Open();
		}

		IDataBossMigrationScope GetTargetScope(DataBossConfiguration config) {
			if(string.IsNullOrEmpty(config.Script)) {
				return new DataBossLogMigrationScope(log, new DataBossSqlMigrationScope(db));
			}
			return config.Script == "con:"
				? new DataBossScriptMigrationScope(Console.Out, false) 
				: new DataBossScriptMigrationScope(new StreamWriter(File.Create(config.Script)), true);
		}

		List<IDataBossMigration> GetPendingMigrations(DataBossConfiguration config) {
			var applied = new HashSet<string>(GetAppliedMigrations(config).Select(x => x.FullId));
			Func<IDataBossMigration, bool> notApplied = x => !applied.Contains(x.Info.FullId);

			return GetTargetMigration(config.Migrations).Flatten()
				.Where(item => item.HasQueryBatches)
				.Where(notApplied)
				.ToList();
		}

		public static IDataBossMigration GetTargetMigration(DataBossMigrationPath[] migrations) {
			return new DataBossCompositeMigration(migrations.ConvertAll(MakeDirectoryMigration));
		}

		static IDataBossMigration MakeDirectoryMigration(DataBossMigrationPath x) {
			return new DataBossDirectoryMigration(
				x.Path, new DataBossMigrationInfo {
					Id = 0,
					Context = x.Context,
					Name = x.Path,
				}
			);
		}

		public List<DataBossMigrationInfo> GetAppliedMigrations(DataBossConfiguration config) {
			using(var cmd = new SqlCommand("select object_id('__DataBossHistory', 'U')", db)) {
				if(cmd.ExecuteScalar() is DBNull)
					throw new InvalidOperationException($"DataBoss has not been initialized, run: {ProgramName} init <target>");
				cmd.CommandText = scripter.Select(typeof(DataBossMigrationInfo), typeof(DataBossHistory));
				using(var reader = cmd.ExecuteReader()) {
					return objectReader.Read<DataBossMigrationInfo>(reader).ToList();
				}
			}
		}
	}
}
