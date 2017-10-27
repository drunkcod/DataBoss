using DataBoss.Data;
using DataBoss.Linq;
using DataBoss.Migrations;
using DataBoss.Schema;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text;

namespace DataBoss
{
	public class DataBoss : IDisposable
	{
		readonly DataBossConfiguration config;
		readonly IDataBossLog log;
		readonly SqlConnection db;
		readonly DataBossScripter scripter = new DataBossScripter();

		DataBoss(DataBossConfiguration config, IDataBossLog log, SqlConnection db) {
			this.config = config;
			this.log = log;
			this.db = db;
		}

		void IDisposable.Dispose() => db.Dispose();

		public static DataBoss Create(DataBossConfiguration config, IDataBossLog log) =>
			new DataBoss(config, log, new SqlConnection(config.GetConnectionString()));

		[DataBossCommand("init")]
		public int Initialize() {
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
			using(var db = new SqlConnection(qs.ConnectionString)) {
				db.Open();
				using(var cmd = new SqlCommand("if db_id(@db) is null select(select database_id from sys.databases where name = @db) else select db_id(@db)" ,db)) {
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
				return new DataBossLogMigrationScope(log, new DataBossMigrationScope(db, new DataBossShellExecute()));
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
					throw new InvalidOperationException($"DataBoss has not been initialized, run: init <target>");
				cmd.CommandText = scripter.Select(typeof(DataBossMigrationInfo), typeof(DataBossHistory));
				using(var reader = ObjectReader.For(cmd.ExecuteReader()))
					return reader.Read<DataBossMigrationInfo>().ToList();
			}
		}
	}
}
