using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
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
			new(config, log, DbConnectionExtensions.Wrap(new SqlConnection(config.GetConnectionString())));

		[DataBossCommand("init")]
		public int Initialize() {
			EnsureDataBase(config.GetConnectionString());
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

			using(var targetScope = GetTargetScope(config)) {
				var migrator = new DataBossMigrator(info => targetScope);
				return migrator.ApplyRange(pending) ? 0 : -1;
			}
		}

		public static void EnsureDataBase(string connectionString) {
			var serverConnectionString = new SqlConnectionStringBuilder(connectionString);
			var dbName = serverConnectionString.InitialCatalog;
			serverConnectionString.InitialCatalog = string.Empty;

			using var cmd = SqlCommandExtensions.Open(serverConnectionString);
			var dbToCreate = cmd.ExecuteScalar("select case when db_id(@db) is null then quotename(@db) else null end", new { db = dbName });
			if(dbToCreate is DBNull)
				return;
			cmd.ExecuteNonQuery($"create database {dbToCreate}");
		}

		void Open() {
			if(db.State == ConnectionState.Open)
				return;
			db.Open();
			switch(GetTableVersion(db, "__DataBossHistory")) {
				case 0:
					CreateDataBossHistoryTable(db);
					goto case 1;
				case 1:
					AddMigrationHashColumn(db);
					goto case 2;
				case 2: break;
			}
		}

		static int GetTableVersion(IDataBossConnection db, string tableName) {
			using var c = db.CreateCommand(
				  "with table_version(table_name, version) as (\n"
				+ "select table_name = tables.name, version = isnull((\n"
				+ "select cast(value as int)\n"
				+ "from sys.extended_properties p\n"
				+ "where p.name = 'version' and p.class = 1 and tables.object_id = p.major_id\n"
				+ "), 1)\n"
				+ "from sys.tables\n"
				+ ")\n"
				+ "select isnull((\n"
				+ "select version\n"
				+ "from table_version\n"
				+ "where table_name = @tableName), 0)", new { tableName });
			return (int)c.ExecuteScalar();
		}

		static void CreateDataBossHistoryTable(IDataBossConnection db) { 
			using var c = db.CreateCommand(
				  "create table [dbo].[__DataBossHistory](\n"
				+ "[Id] bigint not null,\n" 
				+ "[Context] varchar(64) not null,\n"
				+ "[Name] varchar(max) not null,\n"
				+ "[StartedAt] datetime not null,\n"
				+ "[FinishedAt] datetime,\n"
				+ "[User] varchar(max),\n"
				+ "constraint[PK_DataBossHistory] primary key([Id], [Context]))");
			c.ExecuteNonQuery();
		}

		static void AddMigrationHashColumn(IDataBossConnection db) {
			using(var c = db.CreateCommand(
				  "alter table[dbo].[__DataBossHistory]\n"
				+ "add [MigrationHash] binary(32)"))
				c.ExecuteNonQuery();

			using(var c = db.CreateCommand("sp_addextendedproperty", new { 
				name = "version",
				value = 2,
				level0type = "Schema", level0name = "dbo",
				level1type = "Table", level1name = "__DataBossHistory",
			})) {
				c.CommandType = CommandType.StoredProcedure;
				c.ExecuteNonQuery();
			}
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
