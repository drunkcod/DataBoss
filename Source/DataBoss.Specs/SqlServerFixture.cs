using System;
using System.Data.SqlClient;
using System.Threading.Tasks;
using DataBoss.Testing.SqlServer;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Configurations;
using DotNet.Testcontainers.Containers;

namespace DataBoss
{
	class SqlServerTestContainerBuilder
	{
		const int MsSqlPort = 1433;

		public IContainer Build() {
			return new ContainerBuilder()
				.WithImage("mcr.microsoft.com/mssql/server:2019-latest")
				.WithPortBinding(MsSqlPort)
				.WithEnvironment("ACCEPT_EULA", "Y")
				.WithEnvironment("MSSQL_SA_PASSWORD", "Pa55w0rd!")
				.WithWaitStrategy(
					Wait.ForUnixContainer()
					.UntilMessageIsLogged("SQL Server is now ready for client connections.")
					.AddCustomWaitStrategy(new WaitUntilSqlConnectionOk()))
				.Build();
		}

		sealed class WaitUntilSqlConnectionOk : IWaitUntil
		{
			public async Task<bool> UntilAsync(IContainer container) {
				var cs = new TestDbConfig { 
						Port = container.GetMappedPublicPort(MsSqlPort),
						Username = "sa",
						Password = "Pa55w0rd!",
				}.GetConnectionString().ToString();

				using var c = new SqlConnection(cs);
				try {
					await c.OpenAsync().ConfigureAwait(false);
					return true;
				}
				catch {
					return false;
				}
			}
		}
	}
	public sealed class SqlServerFixture : IDisposable
	{
		static readonly IContainer sqlContainer;
		readonly SqlServerTestDb testDb;
		public SqlConnection Connection { get; private set; }
		public string ConnectionString => testDb.ConnectionString;

		public TestDbConfig Config = new() {
			Username = "sa",
			Password = "Pa55w0rd!",
			Port =  1433,
		};

		public SqlConnection Open() {
			var c = new SqlConnection(ConnectionString);
			c.Open();
			return c;
		}

		static SqlServerFixture() {
			sqlContainer = new SqlServerTestContainerBuilder().Build();
		}

		public SqlServerFixture() {
			lock(sqlContainer) {
				if(sqlContainer.State != TestcontainersStates.Running)
					sqlContainer.StartAsync().Wait();
			}
			this.testDb = SqlServerTestDb.Create(Config);
			Connection = new SqlConnection(testDb.ConnectionString);
			Connection.Open();
		}

		void IDisposable.Dispose() {
			Connection.Dispose();
			testDb.Dispose();
		}
	}


}
