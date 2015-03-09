using System;
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
			inner.Done();
			Done();
		}
	}

	class DataBossScriptMigrationScope : IDataBossMigrationScope
	{
		const string BatchSeparator = "GO";
		readonly TextWriter output;
		readonly bool closeOutput;
		long id;

		public DataBossScriptMigrationScope(TextWriter output, bool closeOutput) {
			this.output = output;
			this.closeOutput = closeOutput;
		}

		public void Begin(DataBossMigrationInfo info) {
			id = info.Id;
			Execute(string.Format("insert __DataBossHistory(Id, Context, Name, StartedAt, [User]) values({0}, '{1}', '{2}', getdate(), '{3}')", 
				id, info.Context, info.Name, Environment.UserName));
		}

		public void Execute(string query) {
			output.WriteLine(query);
			output.WriteLine(BatchSeparator);
		}

		public void Done() {
			Execute("update __DataBossHistory set FinishedAt = getdate() where Id = " + id);
			output.Flush();
		}

		void IDisposable.Dispose() { 
			Done(); 
			if(closeOutput)
				output.Close();
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
