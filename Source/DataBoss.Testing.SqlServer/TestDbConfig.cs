using System;
using System.Data.SqlClient;

namespace DataBoss.Testing.SqlServer
{
	public class TestDbConfig
	{
		public string Server;
		public string Name;
		public string Username;
		public string Password;
		public string ApplicationName;

		public static TestDbConfig Finalize(TestDbConfig config)
		{
			if(config == null)
				return Finalize(new TestDbConfig());
			return new TestDbConfig {
				Server = config.Server,
				Name = config.Name ?? RandomName(),
				Username = config.Username,
				Password = config.Password,
				ApplicationName = config.ApplicationName,
			};
		}

		static string RandomName() =>
			Convert.ToBase64String(Guid.NewGuid().ToByteArray()).TrimEnd('=');

		public SqlConnectionStringBuilder GetConnectionString() {
			var cs = GetServerConnectionString();
			cs.InitialCatalog = Name ?? string.Empty;

			if (ApplicationName != null)
				cs.ApplicationName = ApplicationName;

			return cs;
		}

		public override string ToString() => 
			GetConnectionString().ToString();

		internal SqlConnectionStringBuilder GetServerConnectionString() {
			var cs = new SqlConnectionStringBuilder {
				DataSource = Server ?? ".",
			};

			if (Username != null) {
				cs.UserID = Username;
				cs.Password = Password;
			} else cs.IntegratedSecurity = true;

			return cs;
		}
	}
}
