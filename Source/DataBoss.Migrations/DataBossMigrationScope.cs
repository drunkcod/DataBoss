using System;
using System.Data;
using System.IO;
using DataBoss.Data;

namespace DataBoss.Migrations
{
	public class DataBossMigrationScope : IDataBossMigrationScope
	{
		readonly DataBossMigrationScopeContext scopeContext;
		readonly IDataBossConnection db;
		readonly DataBossShellExecute shellExecute;
		IDbCommand cmd;
		bool isFaulted;

		public DataBossMigrationScope(
			DataBossMigrationScopeContext scopeContext,
			IDataBossConnection db, 
			DataBossShellExecute shellExecute) {
			this.scopeContext = scopeContext;
			this.db = db;
			this.shellExecute = shellExecute;
		}

		public event EventHandler<ErrorEventArgs> OnError;

		public void Begin(DataBossMigrationInfo info) {
			cmd = db.CreateCommand(
				  "update __DataBossHistory with(holdlock)\n"
				+ "set [StartedAt] = getdate(), [FinishedAt] = null, [MigrationHash] = @hash\n"
				+ "where Id = @id and Context = @context\n"
				+ "if @@rowcount = 0\n"
				+ "  insert __DataBossHistory(Id, Context, Name, StartedAt, [User], [MigrationHash])\n"
				+ "  values(@id, @context, @name, getdate(), @user, @hash)", new {
				id = info.Id,
				context = info.Context,
				name = info.Name,
				user = Environment.UserName,
				hash = info.MigrationHash,
			});
			cmd.Transaction = db.BeginTransaction("LikeABoss");
			cmd.ExecuteNonQuery();
		}

		public bool Execute(DataBossQueryBatch query) {
			if(isFaulted)
				return false;
			try {
				return query.BatchType switch {
					DataBossQueryBatchType.Query => ExecuteQuery(query),
					DataBossQueryBatchType.ExternalCommand => ExecuteCommand(query),
					_ => false,
				};
			} catch(Exception e) {
				isFaulted = true;
				OnError.Raise(this, new ErrorEventArgs(e));
				return false;
			}
		}

		private bool ExecuteQuery(DataBossQueryBatch query) {
			using var q = db.CreateCommand(query.ToString());
			q.CommandTimeout = 0;
			q.Transaction = cmd.Transaction;			
			q.ExecuteNonQuery();
			return true;
		}

		private bool ExecuteCommand(DataBossQueryBatch command) =>
			shellExecute.Execute(string.IsNullOrEmpty(command.Path) ? string.Empty: Path.GetDirectoryName(command.Path), command.ToString(),
				("DATABOSS_CONNECTION", scopeContext.ConnectionString),
				("DATABOSS_DATABASE", scopeContext.Database),
				("DATABOSS_SERVER", scopeContext.Server));

		public void Done() {
			if(cmd == null)
				return;
			if(!isFaulted) {
				cmd.CommandText = "update __DataBossHistory set FinishedAt = getdate() where Id = @id and Context = @Context";
				cmd.ExecuteNonQuery();
				cmd.Transaction.Commit();
			} else
				cmd.Transaction?.Rollback();
		}

		void IDisposable.Dispose() {
			if(cmd == null)
				return;
			cmd.Dispose();
			cmd = null;
		}
	}
}