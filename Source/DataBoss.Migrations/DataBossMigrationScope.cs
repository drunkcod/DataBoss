using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;
using DataBoss.Linq;

namespace DataBoss.Migrations
{
	public class DataBossMigrationScope : IDataBossMigrationScope
	{
		readonly DataBossMigrationScopeContext scopeContext;
		readonly SqlConnection db;
		readonly DataBossShellExecute shellExecute;
		SqlCommand cmd;
		bool isFaulted;

		public DataBossMigrationScope(
			DataBossMigrationScopeContext scopeContext,
			SqlConnection db, 
			DataBossShellExecute shellExecute) {
			this.scopeContext = scopeContext;
			this.db = db;
			this.shellExecute = shellExecute;
		}

		public event EventHandler<ErrorEventArgs> OnError;

		public void Begin(DataBossMigrationInfo info) {
			cmd = new SqlCommand(
				"insert __DataBossHistory(Id, Context, Name, StartedAt, [User]) values(@id, @context, @name, getdate(), @user)",
				db, 
				db.BeginTransaction("LikeABoss"));

			cmd.Parameters.AddWithValue("@id", info.Id);
			cmd.Parameters.AddWithValue("@context", info.Context ?? string.Empty);
			cmd.Parameters.AddWithValue("@name", info.Name);
			cmd.Parameters.AddWithValue("@user", Environment.UserName);
			cmd.ExecuteNonQuery();
		}

		public bool Execute(DataBossQueryBatch query) {
			if(isFaulted)
				return false;
			try {
				switch(query.BatchType) {
					default: return false;
					case DataBossQueryBatchType.Query: return ExecuteQuery(query);
					case DataBossQueryBatchType.ExternalCommand: return ExecuteCommand(query);
				}
			} catch(Exception e) {
				isFaulted = true;
				OnError.Raise(this, new ErrorEventArgs(e));
				return false;
			}
		}

		private bool ExecuteQuery(DataBossQueryBatch query) {
			using (var q = new SqlCommand(query.ToString(), db, cmd.Transaction) { CommandTimeout = 0 }) {
					q.ExecuteNonQuery();
					return true;
			}
		}

		private bool ExecuteCommand(DataBossQueryBatch command) =>
			shellExecute.Execute(string.IsNullOrEmpty(command.Path) ? string.Empty: Path.GetDirectoryName(command.Path), command.ToString(), new [] {
				KeyValuePair.Create("DATABOSS_CONNECTION", scopeContext.ConnectionString),
				KeyValuePair.Create("DATABOSS_DATABASE", scopeContext.Database),
				KeyValuePair.Create("DATABOSS_SERVER", scopeContext.Server),
			});

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