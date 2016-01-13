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
			Execute($"insert __DataBossHistory(Id, Context, Name, StartedAt, [User]) values({id}, '{info.Context}', '{info.Name}', getdate(), '{Environment.UserName}')");
		}

		public bool Execute(string query) {
			output.WriteLine(query);
			output.WriteLine(BatchSeparator);
			return true;
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
}