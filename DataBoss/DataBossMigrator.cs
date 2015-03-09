using System;
using System.IO;

namespace DataBoss
{
	public interface IDataBossMigrationScope : IDisposable
	{
		void Begin(DataBossMigrationInfo info);
		void Execute(string query);
		void Done();
	}

	class DataBossScriptMigrationScope : IDataBossMigrationScope
	{
		const string BatchSeparator = "GO";
		readonly TextWriter output;
		readonly bool closeOutput;
		long id;
		string context;

		public DataBossScriptMigrationScope(TextWriter output, bool closeOutput) {
			this.output = output;
			this.closeOutput = closeOutput;
		}

		public void Begin(DataBossMigrationInfo info) {
			id = info.Id;
			context = info.Context;
			Execute(string.Format("insert __DataBossHistory(Id, Context, Name, StartedAt, [User]) values({0}, '{1}', '{2}', getdate(), '{3}')", 
				id, info.Context, info.Name, Environment.UserName));
		}

		public void Execute(string query) {
			output.WriteLine(query);
			output.WriteLine(BatchSeparator);
		}

		public void Done() {
			Execute("update __DataBossHistory set FinishedAt = getdate() where Id = " + id + " and Context = '" + context + "'");
			output.Flush();
		}

		void IDisposable.Dispose() { 
			output.Dispose();
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
			var scope = scopeFactory(migration.Info);
			try {
				scope.Begin(migration.Info);
				foreach(var query in migration.GetQueryBatches())
					scope.Execute(query);
			} finally {
				scope.Done();
			}
		}
	}
}
