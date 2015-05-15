using System;
using System.Data.SqlClient;
using System.IO;

namespace DataBoss
{
	public class DataBossSqlMigrationScope : IDataBossMigrationScope
	{
		readonly SqlConnection db;
		bool hasErrors;
		SqlCommand cmd;

		public DataBossSqlMigrationScope(SqlConnection db) {
			this.db = db;
		}

		public event EventHandler<ErrorEventArgs> OnError;

		public void Begin(DataBossMigrationInfo info) {
			cmd = new SqlCommand("insert __DataBossHistory(Id, Context, Name, StartedAt, [User]) values(@id, @context, @name, getdate(), @user)", db, db.BeginTransaction("LikeABoss"));

			cmd.Parameters.AddWithValue("@id", info.Id);
			cmd.Parameters.AddWithValue("@context", info.Context ?? string.Empty);
			cmd.Parameters.AddWithValue("@name", info.Name);
			cmd.Parameters.AddWithValue("@user", Environment.UserName);
			cmd.ExecuteNonQuery();
		}

		public void Execute(string query) {
			if(hasErrors)
				return;

			using(var q = new SqlCommand(query, db, cmd.Transaction))
				try { q.ExecuteNonQuery(); }
				catch(SqlException e) { 
					hasErrors = true;
 					if(OnError != null)
						OnError(this, new ErrorEventArgs(e));
				}
		}

		public void Done() {
			if(cmd == null)
				return;
			if(!hasErrors) {
				cmd.CommandText = "update __DataBossHistory set FinishedAt = getdate() where Id = @id and Context = @Context";
				cmd.ExecuteNonQuery();
				cmd.Transaction.Commit();
			} else {
				if(cmd.Transaction != null)
					cmd.Transaction.Rollback();
			}
		}

		void IDisposable.Dispose() {
			if(cmd != null) {
				cmd.Dispose();				
				cmd = null;
			}
		}
	}
}