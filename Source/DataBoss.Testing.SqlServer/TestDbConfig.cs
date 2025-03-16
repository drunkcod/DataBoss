using System;
using Microsoft.Data.SqlClient;

namespace DataBoss.Testing.SqlServer
{
	public class TestDbConfig
	{
		public string Server;
		public string Name;
		public string Username;
		public string Password;
		public string ApplicationName;
		public int? Port;

		public TestDbConfig WithName(string name) => new() {
			Name = name,
			Server = this.Server,
			Username = this.Username,
			Password = this.Password,
			ApplicationName = this.ApplicationName,
			Port = this.Port,
		};

		public static TestDbConfig Finalize(TestDbConfig config) {
			if (config == null)
				return Finalize(new TestDbConfig());

			if (string.IsNullOrEmpty(config.Name))
				return config.WithName(RandomName());

			return config;
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
				DataSource = DataSource,
			};

			if (Username != null) {
				cs.UserID = Username;
				cs.Password = Password;
			}
			else cs.IntegratedSecurity = true;
			return cs;
		}

		string DataSource {
			get {
				var useDefaultPort = Port == null || Port == 1433;
				if (string.IsNullOrEmpty(Server))
					return useDefaultPort ? "." : $"localhost,{Port}";
				return useDefaultPort ? Server : $"{Server},{Port}";
			}
		}
	}
}
