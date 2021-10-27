using System;
using System.IO;

namespace DataBoss.Migrations
{
	public class DataBossScriptMigrationScope : IDataBossMigrationScope
	{
		const string BatchSeparator = "GO";

		readonly DataBossMigrationScopeContext scopeContext;
		readonly TextWriter output;
		readonly bool closeOutput;
		
		long id;
		string context;

		public DataBossScriptMigrationScope(DataBossMigrationScopeContext scopeContext, TextWriter output, bool closeOutput) {
			this.scopeContext = scopeContext;
			this.output = output;
			this.closeOutput = closeOutput;
		}

		public event EventHandler<ErrorEventArgs> OnError;

		public void Begin(DataBossMigrationInfo info) {
			id = info.Id;
			context = info.Context;
			Execute(DataBossQueryBatch.Query($"insert __DataBossHistory(Id, Context, Name, StartedAt, [User]) values({id}, '{info.Context}', '{info.Name}', getdate(), '{Environment.UserName}')", string.Empty));
		}

		public bool Execute(DataBossQueryBatch query) {
			try {
				output.WriteLine(query);
				output.WriteLine(BatchSeparator);
				return true;
			} catch(Exception e) {
				OnError?.Invoke(this, new ErrorEventArgs(e));
				return false;
			}
		}

		public void Done() {
			Execute(DataBossQueryBatch.Query("update __DataBossHistory set FinishedAt = getdate() where Id = " + id + " and Context = '" + context + "'", string.Empty));
			output.Flush();
		}

		void IDisposable.Dispose() { 
			output.Dispose();
			if(closeOutput)
				output.Close();
		}
	}
}