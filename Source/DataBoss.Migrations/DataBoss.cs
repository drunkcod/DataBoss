using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using DataBoss.Data;
using DataBoss.Linq;
using DataBoss.Migrations;

namespace DataBoss
{
	public class DataBoss : IDisposable
	{
		readonly IDataBossConfiguration config;
		readonly IDataBossLog log;
		readonly IDataBossConnection db;

		DataBoss(IDataBossConfiguration config, IDataBossLog log, IDataBossConnection db) {
			this.config = config;
			this.log = log;
			this.db = db;
		}

		public void Dispose() => db.Dispose();

		public static DataBoss Create(IDataBossConfiguration config, IDataBossLog log) =>
			new(config, log, config.GetConnection());

		[DataBossCommand("init")]
		public int Initialize() {
			using var connection = config.GetConnection();
			EnsureDataBase(connection);
			Open();
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

			using var targetScope = GetTargetScope(config); 
			var migrator = new DataBossMigrator(info => targetScope);
			return migrator.ApplyRange(pending) ? 0 : -1;
		}

		public static void EnsureDataBase(IDataBossConnection connection) =>
			connection.EnsureDatabase();

		void Open() {
			if(db.State == ConnectionState.Open)
				return;
			db.Open();
			for(var i = db.GetTableVersion("__DataBossHistory"); i != db.Dialect.DataBossHistoryMigrations.Count;) {
				using var c = db.CreateCommand(db.Dialect.DataBossHistoryMigrations[i]);	
				c.ExecuteNonQuery();			
				db.SetTableVersion("__DataBossHistory", ++i);
			}

			var userSchema = db.GetDefaultSchema();
			var configSchema = config.DefaultSchema ?? "dbo";
			if (string.Compare(db.GetDefaultSchema(), configSchema, ignoreCase: true) != 0)
				throw new InvalidOperationException(
					  $"User default schema '{userSchema}' doesn't match '{configSchema}'.\n"
					+ $"Either update the user default schema or add 'defaultSchema=\"{userSchema}\"' to the top level db element.");
		}

		IDataBossMigrationScope GetTargetScope(IDataBossConfiguration config) {
			var scopeContext = new DataBossMigrationScopeContext(config.GetConnectionString(), config.Database, config.Server);

			if (!string.IsNullOrEmpty(config.Script))
				return config.Script == "con:"
					? new DataBossScriptMigrationScope(scopeContext, Console.Out, false) 
					: new DataBossScriptMigrationScope(scopeContext, new StreamWriter(File.Create(config.Script)), true);
			
			var shell = new DataBossShellExecute();
			shell.OutputDataReceived += (_, e) => Console.WriteLine(e.Data);
			return new DataBossLogMigrationScope(log, new DataBossMigrationScope(scopeContext, db, shell));
		}

		List<IDataBossMigration> GetPendingMigrations(IDataBossConfiguration config) {
			var applied = GetAppliedMigrations().ToDictionary(x => x.FullId, x => x.MigrationHash);
			bool NotApplied(IDataBossMigration x) => !(x.IsRepeatable 
				? applied.TryGetValue(x.Info.FullId, out var hash) && new Span<byte>(hash).SequenceEqual(x.Info.MigrationHash)
				: applied.ContainsKey(x.Info.FullId));

			return config.GetTargetMigration()
				.Flatten()
				.Where(item => item.HasQueryBatches)
				.Where(NotApplied)
				.ToList();
		}

		public List<DataBossMigrationInfo> GetAppliedMigrations() {
			using var cmd = db.CreateCommand();
			return cmd.ExecuteQuery<DataBossMigrationInfo>("select * from __DataBossHistory").ToList();
		}
	}
}
