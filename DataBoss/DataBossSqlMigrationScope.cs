using System;
using System.Data.SqlClient;
using System.Linq.Expressions;

namespace DataBoss
{
	class DataBossSqlMigrationScope : IDataBossMigrationScope
	{
		readonly SqlConnection db;
		SqlCommand cmd;

		public DataBossSqlMigrationScope(SqlConnection db) {
			this.db = db;
		}

		public void Begin(DataBossMigrationInfo info) {
			this.cmd = new SqlCommand("insert __DataBossHistory(Id, Context, Name, StartedAt, [User]) values(@id, @context, @name, getdate(), @user)", db);
			cmd.Parameters.AddWithValue("@id", info.Id);
			cmd.Parameters.AddWithValue("@context", info.Context ?? string.Empty);
			cmd.Parameters.AddWithValue("@name", info.Name);
			cmd.Parameters.AddWithValue("@user", Environment.UserName);
			cmd.ExecuteNonQuery();
		}

		public void Execute(string query) {
			using(var q = new SqlCommand(query, db))
				q.ExecuteNonQuery();
		}

		public void Done() {
			if(cmd == null)
				return;

			cmd.CommandText = "update __DataBossHistory set FinishedAt = getdate() where Id = @id and Context = @Context";
			cmd.ExecuteNonQuery();			
		}

		void IDisposable.Dispose() {
			if(cmd != null) {
				cmd.Dispose();				
				cmd = null;
			}
		}
	}
}