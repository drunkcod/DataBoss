using System;
using System.Data.SqlClient;
using System.Diagnostics;
using System.IO;

namespace DataBoss
{
	public interface IDataBossMigrationScope : IDisposable
	{
		void Begin(DataBossMigrationInfo info);
		void Execute(string query);
		void Done();
	}

	class DataBossConsoleLogMigrationScope : IDataBossMigrationScope
	{
		readonly IDataBossMigrationScope inner;
		readonly Stopwatch stopwatch;

		public DataBossConsoleLogMigrationScope(IDataBossMigrationScope inner) {
			this.inner = inner;
			this.stopwatch = new Stopwatch();
		}

		public void Begin(DataBossMigrationInfo info) {
			Console.WriteLine("  Applying: {0}. {1}", info.Id, info.Name);
			stopwatch.Restart();
			inner.Begin(info);
		}

		public void Execute(string query) { inner.Execute(query); }

		public void Done() {
			Console.WriteLine("    Finished in {0}", stopwatch.Elapsed);
		}

		void IDisposable.Dispose() {
			Done();
		}
	}

	class DataBossSqlMigrationScope : IDataBossMigrationScope
	{
		readonly SqlConnection db;
		SqlCommand cmd;

		public DataBossSqlMigrationScope(SqlConnection db) {
			this.db = db;
		}

		public void Begin(DataBossMigrationInfo info) {
			this.cmd = new SqlCommand("insert __DataBossHistory(Id, Context, Name, StartedAt, [User]) values(@id, @context, @name, getdate(), @user)", db);
			cmd.Parameters.AddWithValue("@id", info.Id);
			cmd.Parameters.AddWithValue("@context", info.Context ?? string.Empty);
			cmd.Parameters.AddWithValue("@name", info.Name);
			cmd.Parameters.AddWithValue("@user", Environment.UserName);
			cmd.ExecuteNonQuery();
		}

		public void Execute(string query) {
			using(var q = new SqlCommand(query, db))
				q.ExecuteNonQuery();
		}

		public void Done() {
			if(cmd == null)
				return;

			cmd.CommandText = "update __DataBossHistory set FinishedAt = getdate() where Id = @id";
			cmd.ExecuteNonQuery();			
			cmd.Dispose();				
			cmd = null;
		}

		void IDisposable.Dispose() {
			Done();
		}
	}

	class DataBossMigrator 
	{
		readonly Func<DataBossMigrationInfo, IDataBossMigrationScope> scopeFactory;

		public DataBossMigrator(Func<DataBossMigrationInfo, IDataBossMigrationScope> scopeFactory) {
			this.scopeFactory = scopeFactory;
		}

		public void Apply(IDataBossMigration migration) {
			using(var scope = scopeFactory(migration.Info)) {
				scope.Begin(migration.Info);
				foreach(var query in migration.GetQueryBatches())
					scope.Execute(query);
			}
		}
	}
}
