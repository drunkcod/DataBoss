using System;
using System.IO;

namespace DataBoss
{
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

		public event EventHandler<ErrorEventArgs> OnError;

		public void Begin(DataBossMigrationInfo info) {
			id = info.Id;
			context = info.Context;
			Execute(new DataBossQueryBatch($"insert __DataBossHistory(Id, Context, Name, StartedAt, [User]) values({id}, '{info.Context}', '{info.Name}', getdate(), '{Environment.UserName}')"));
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
			Execute(new DataBossQueryBatch("update __DataBossHistory set FinishedAt = getdate() where Id = " + id + " and Context = '" + context + "'"));
			output.Flush();
		}

		void IDisposable.Dispose() { 
			output.Dispose();
			if(closeOutput)
				output.Close();
		}
	}
}