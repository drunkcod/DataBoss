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
			this.cmd = new SqlCommand("set xact_abort on\nbegin transaction", db);
			cmd.ExecuteNonQuery();
			cmd.CommandText = "insert __DataBossHistory(Id, Context, Name, StartedAt, [User]) values(@id, @context, @name, getdate(), @user)";
			cmd.Parameters.AddWithValue("@id", info.Id);
			cmd.Parameters.AddWithValue("@context", info.Context ?? string.Empty);
			cmd.Parameters.AddWithValue("@name", info.Name);
			cmd.Parameters.AddWithValue("@user", Environment.UserName);
			cmd.ExecuteNonQuery();
		}

		public void Execute(string query) {
			if(hasErrors)
				return;

			using(var q = new SqlCommand(query, db))
				try { q.ExecuteNonQuery(); }
				catch(Exception e) { 
					hasErrors = true;
 					if(OnError != null)
						OnError(this, new ErrorEventArgs(e));
				}
		}

		public void Done() {
			if(cmd == null)
				return;
			if(!hasErrors) {
				cmd.CommandText = "update __DataBossHistory set FinishedAt = getdate() where Id = @id and Context = @Context\ncommit";
				cmd.ExecuteNonQuery();
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