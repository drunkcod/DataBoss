using System;
using System.Data.SqlClient;
using System.IO;

namespace DataBoss
{
	public class DataBossSqlMigrationScope : IDataBossMigrationScope
	{
		readonly SqlConnection db;
		SqlCommand cmd;
		bool isFaulted;

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

		public bool Execute(string query) {
			if(isFaulted)
				return false;

			using(var q = new SqlCommand(query, db, cmd.Transaction))
				try {
					q.ExecuteNonQuery();
					return true;
				} catch(SqlException e) { 
					isFaulted = true;
					OnError.Raise(this, new ErrorEventArgs(e));
					return false;
				}
		}

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