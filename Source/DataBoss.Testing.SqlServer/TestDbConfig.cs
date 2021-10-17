using System.Data.SqlClient;

namespace DataBoss.Testing.SqlServer
{
	public class TestDbConfig
	{
		public string Server;
		public string Name;
		public string Username;
		public string Password;
		
		public SqlConnectionStringBuilder GetConnectionString() {
			var cs = GetServerConnectionString();
			cs.InitialCatalog = Name ?? string.Empty;
			return cs;
		}

		public override string ToString() => 
			GetConnectionString().ToString();

		internal SqlConnectionStringBuilder GetServerConnectionString() {
			var cs = new SqlConnectionStringBuilder {
				ApplicationName = typeof(SqlServerTestDb).FullName,
				DataSource = Server ?? ".",
			};
			if (Username != null) {
				cs.UserID = Username;
				cs.Password = Password;
			}
			else cs.IntegratedSecurity = true;
			return cs;
		}
	}
}
