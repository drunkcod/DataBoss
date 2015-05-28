using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using DataBoss.Schema;

namespace DataBoss
{
    public class Program
	{
		static string ProgramName => Path.GetFileName(typeof(Program).Assembly.Location);

	    readonly SqlConnection db;

		static string ReadResource(string path) {
			using(var reader = new StreamReader(typeof(Program).Assembly.GetManifestResourceStream(path)))
				return reader.ReadToEnd();
		}

		public Program(SqlConnection db) {
			this.db = db;
		}

		static int Main(string[] args) {
			if(args.Length == 0) {
				Console.WriteLine(GetUsageString());
				return 0;
			}

			try {
				var cc = DataBossConfiguration.ParseCommandConfig(args);
                
                Action<Program, DataBossConfiguration> command;
				if(!TryGetCommand(cc.Key, out command)) {
					Console.WriteLine(GetUsageString());
					return -1;
				}

				using(var db = new SqlConnection(cc.Value.GetConnectionString())) {
					command(new Program(db), cc.Value);
				}

			} catch(Exception e) {
				WriteError(e);
				Console.Error.WriteLine();
				Console.Error.WriteLine(GetUsageString());
				return -1;
			}

			return 0;
		}

		static bool TryGetCommand(string name, out Action<Program, DataBossConfiguration> command) {
			var commands = new Dictionary<string, Action<Program, DataBossConfiguration>> {
				{ "init", (p, c) => p.Initialize(c) },
				{ "status", (p, c) => p.Status(c) },
				{ "update", (p, c) => p.Update(c) },
			};

			return commands.TryGetValue(name, out command);
		}

		private static void WriteError(Exception e)
		{
			var oldColor = Console.ForegroundColor;
			Console.ForegroundColor = ConsoleColor.Red;
			Console.Error.WriteLine(e.Message);
			Console.ForegroundColor = oldColor;
		}

		static string GetUsageString() {
			return ReadResource("Usage")
				.Replace("{{ProgramName}}", ProgramName)
				.Replace("{{Version}}", typeof(Program).Assembly.GetName().Version.ToString());					
		}

		public void Initialize(DataBossConfiguration config) {
			EnsureDatabse(config.GetConnectionString());
			var scripter = new DataBossScripter();
			using(var cmd = new SqlCommand(scripter.CreateMissing(typeof(DataBossHistory)), db))
			{
				Open();
				using(var r = cmd.ExecuteReader())
					while(r.Read())
						Console.WriteLine(r.GetValue(0));
			}
		}

		static void EnsureDatabse(string connectionString) {
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

		void Status(DataBossConfiguration config) {
			Open();
			var pending = GetPendingMigrations(config);
			if(pending.Count != 0) {
				Console.WriteLine("Pending migrations:");
				foreach(var item in pending)
					Console.WriteLine("  {0} - {1}", item.Info.FullId, item.Info.Name);
			}
		}

		void Update(DataBossConfiguration config) {
			Open();
			var pending = GetPendingMigrations(config);
			Console.WriteLine("{0} pending migrations found.", pending.Count);

			using(var targetScope = GetTargetScope(config)) {
				var migrator = new DataBossMigrator(info => targetScope);
				pending.ForEach(migrator.Apply);
			}
		}

		void Open() {
			if(db.State != ConnectionState.Open)
				db.Open();
		}

		IDataBossMigrationScope GetTargetScope(DataBossConfiguration config) {
			if(string.IsNullOrEmpty(config.Script)) {
				return new DataBossConsoleLogMigrationScope(
					new DataBossSqlMigrationScope(db));
			}
			if(config.Script == "con:")
				return new DataBossScriptMigrationScope(Console.Out, false);
			return new DataBossScriptMigrationScope(new StreamWriter(File.Create(config.Script)), true);
		}

		private List<IDataBossMigration> GetPendingMigrations(DataBossConfiguration config) {
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

		public IEnumerable<DataBossMigrationInfo> GetAppliedMigrations(DataBossConfiguration config) {
			using(var cmd = new SqlCommand("select isnull(object_id('__DataBossHistory', 'U'), 0)", db)) {
				if((int)cmd.ExecuteScalar() == 0)
					throw new InvalidOperationException($"DataBoss has not been initialized, run: {ProgramName} init <target>");

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
