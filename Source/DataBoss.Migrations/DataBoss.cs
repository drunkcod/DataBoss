using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text;
using DataBoss.Data;
using DataBoss.Data.Scripting;
using DataBoss.Linq;
using DataBoss.Migrations;
using DataBoss.Schema;

namespace DataBoss
{
	public class DataBoss : IDisposable
	{
		readonly IDataBossConfiguration config;
		readonly IDataBossLog log;
		readonly IDataBossConnection db;
		readonly DataBossScripter scripter;

		DataBoss(IDataBossConfiguration config, IDataBossLog log, IDataBossConnection db) {
			this.config = config;
			this.log = log;
			this.db = db;
			this.scripter = new DataBossScripter(db.Dialect);
		}

		public void Dispose() => db.Dispose();

		public static DataBoss Create(IDataBossConfiguration config, IDataBossLog log) =>
			new(config, log, DbConnectionExtensions.Wrap(new SqlConnection(config.GetConnectionString())));

		[DataBossCommand("init")]
		public int Initialize() {
			EnsureDataBase(config.GetConnectionString());
			using(var cmd = db.CreateCommand(scripter.CreateMissing(typeof(DataBossHistory))))
			{
				Open();
				using var r = cmd.ExecuteReader();
				while (r.Read())
					log.Info("{0}", r.GetValue(0));
			}
			return 0;
		}

		[DataBossCommand("list")]
		public int ListMigrations() {
			var message = new StringBuilder();
			message.AppendLine("Found migrations:");
			foreach (var item in config.GetTargetMigration().Flatten().Where(x => x.HasQueryBatches))
				message.AppendFormat("  {0} - {1}\n", item.Info.FullId, item.Info.Name);
			log.Info(message.ToString());
			return 0;
		}

		[DataBossCommand("status")]
		public int Status() {
			Open();
			var pending = GetPendingMigrations(config);
			if(pending.Count != 0) {
				var message = new StringBuilder();
				message.AppendLine("Pending migrations:");
				foreach(var item in pending)
					message.AppendFormat("  {0} - {1}\n", item.Info.FullId, item.Info.Name);
				log.Info(message.ToString());
			}
			return pending.Count;
		}

		[DataBossCommand("update")]
		public int Update() {
			Open();
			var pending = GetPendingMigrations(config);
			log.Info("{0} pending migrations found.", pending.Count);

			using(var targetScope = GetTargetScope(config)) {
				var migrator = new DataBossMigrator(info => targetScope);
				return migrator.ApplyRange(pending) ? 0 : -1;
			}
		}

		public static void EnsureDataBase(string connectionString) {
			var qs = new SqlConnectionStringBuilder(connectionString);
			var dbName = qs.InitialCatalog;
			qs.Remove("Initial Catalog");

			using var cmd = SqlCommandExtensions.Open(qs.ConnectionString);
			if(cmd.ExecuteScalar("if db_id(@db) is null select(select database_id from sys.databases where name = @db) else select db_id(@db)", new { db = dbName }) is DBNull) {
				cmd.ExecuteNonQuery($"create database [{dbName}]");
			}
		}

		void Open() {
			if(db.State != ConnectionState.Open)
				db.Open();
		}

		IDataBossMigrationScope GetTargetScope(IDataBossConfiguration config) {
			var scopeContext = DataBossMigrationScopeContext.From(config.GetConnectionString());

			if (!string.IsNullOrEmpty(config.Script))
				return config.Script == "con:"
					? new DataBossScriptMigrationScope(scopeContext, Console.Out, false) 
					: new DataBossScriptMigrationScope(scopeContext, new StreamWriter(File.Create(config.Script)), true);
			
			var shell = new DataBossShellExecute();
			shell.OutputDataReceived += (_, e) => Console.WriteLine(e.Data);
			return new DataBossLogMigrationScope(log, new DataBossMigrationScope(scopeContext, db, shell));
		}

		List<IDataBossMigration> GetPendingMigrations(IDataBossConfiguration config) {
			var applied = new HashSet<string>(GetAppliedMigrations().Select(x => x.FullId));
			bool NotApplied(IDataBossMigration x) => !applied.Contains(x.Info.FullId);

			return config.GetTargetMigration()
				.Flatten()
				.Where(item => item.HasQueryBatches)
				.Where(NotApplied)
				.ToList();
		}

		public List<DataBossMigrationInfo> GetAppliedMigrations() {
			using var cmd = db.CreateCommand("select object_id('__DataBossHistory', 'U')"); 
			
			if (cmd.ExecuteScalar() is DBNull)
				throw new InvalidOperationException($"DataBoss has not been initialized, run: init <target>");
			
			cmd.CommandText = scripter.Select(typeof(DataBossMigrationInfo), typeof(DataBossHistory));
			
			using var reader = cmd.ExecuteReader();
			return reader.Read<DataBossMigrationInfo>().ToList();
		}
	}
}
