using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using DataBoss.Testing.SqlServer;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Configurations;
using DotNet.Testcontainers.Containers;
using DotNet.Testcontainers.Images;
using Microsoft.Extensions.Logging;

namespace DataBoss
{
	public class SqlServerContainer : IContainer
	{
		readonly IContainer container;
		internal SqlServerContainer(IContainer container) {
			this.container = container;
		}

		public string Username => "sa";
		public string Password => "Pa55w0rd!";
		public ushort Port => GetMappedPublicPort(SqlServerContainerBuilder.DefaultPort);

		public SqlConnectionStringBuilder GetConnectionString() => new() {
			UserID = Username,
			Password = Password,
			DataSource = GetDataSource(Port),
		};

		static string GetDataSource(int port) => port == SqlServerContainerBuilder.DefaultPort ? "." : $"localhost,{port}";

		public ILogger Logger => container.Logger;
		public string Id => container.Id;
		public string Name => container.Name;
		public string IpAddress => container.IpAddress;
		public string MacAddress => container.MacAddress;
		public string Hostname => container.Hostname;
		public IImage Image => container.Image;
		public TestcontainersStates State => container.State;
		public TestcontainersHealthStatus Health => container.Health;
		public long HealthCheckFailingStreak => container.HealthCheckFailingStreak;

		public DateTime CreatedTime => container.CreatedTime;
		public DateTime StartedTime => container.StartedTime;
		public DateTime StoppedTime => container.StoppedTime;
		public DateTime PausedTime => container.PausedTime;
		public DateTime UnpausedTime => container.UnpausedTime;

		public event EventHandler Creating {
			add { container.Creating += value; }
			remove { container.Creating -= value; }
		}

		public event EventHandler Starting {
			add { container.Starting += value; }
			remove { container.Starting -= value; }
		}

		public event EventHandler Stopping {
			add { container.Stopping += value; }
			remove { container.Stopping -= value; }
		}

		public event EventHandler Created {
			add { container.Created += value; }
			remove { container.Created -= value; }
		}

		public event EventHandler Started {
			add { container.Started += value; }
			remove { container.Started -= value; }
		}

		public event EventHandler Stopped {
			add { container.Stopped += value; }
			remove { container.Stopped -= value; }
		}

		public event EventHandler Pausing {
			add { container.Pausing += value; }
			remove { container.Pausing -= value; }
		}

		public event EventHandler Unpausing {
			add { container.Unpausing += value; }
			remove { container.Unpausing -= value; }
		}

		public event EventHandler Paused {
			add { container.Paused += value; }
			remove { container.Paused -= value; }
		}

		public event EventHandler Unpaused {
			add { container.Unpaused += value; }
			remove { container.Unpaused -= value; }
		}

		public Task CopyAsync(byte[] fileContent, string filePath, UnixFileModes fileMode = UnixFileModes.OtherRead | UnixFileModes.GroupRead | UnixFileModes.UserWrite | UnixFileModes.UserRead, CancellationToken ct = default) {
			return container.CopyAsync(fileContent, filePath, fileMode, ct);
		}

		public Task CopyAsync(string source, string target, UnixFileModes fileMode = UnixFileModes.OtherRead | UnixFileModes.GroupRead | UnixFileModes.UserWrite | UnixFileModes.UserRead, CancellationToken ct = default) {
			return container.CopyAsync(source, target, fileMode, ct);
		}

		public Task CopyAsync(DirectoryInfo source, string target, UnixFileModes fileMode = UnixFileModes.OtherRead | UnixFileModes.GroupRead | UnixFileModes.UserWrite | UnixFileModes.UserRead, CancellationToken ct = default) {
			return container.CopyAsync(source, target, fileMode, ct);
		}

		public Task CopyAsync(FileInfo source, string target, UnixFileModes fileMode = UnixFileModes.OtherRead | UnixFileModes.GroupRead | UnixFileModes.UserWrite | UnixFileModes.UserRead, CancellationToken ct = default) {
			return container.CopyAsync(source, target, fileMode, ct);
		}

		public ValueTask DisposeAsync() {
			return container.DisposeAsync();
		}

		public Task<ExecResult> ExecAsync(IList<string> command, CancellationToken ct = default) {
			return container.ExecAsync(command, ct);
		}

		public Task<long> GetExitCodeAsync(CancellationToken ct = default) {
			return container.GetExitCodeAsync(ct);
		}

		public Task<(string Stdout, string Stderr)> GetLogsAsync(DateTime since = default, DateTime until = default, bool timestampsEnabled = true, CancellationToken ct = default) {
			return container.GetLogsAsync(since, until, timestampsEnabled, ct);
		}

		public ushort GetMappedPublicPort(int containerPort) {
			return container.GetMappedPublicPort(containerPort);
		}

		public ushort GetMappedPublicPort(string containerPort) {
			return container.GetMappedPublicPort(containerPort);
		}

		public Task<byte[]> ReadFileAsync(string filePath, CancellationToken ct = default) {
			return container.ReadFileAsync(filePath, ct);
		}

		public Task StartAsync(CancellationToken ct = default) {
			return container.StartAsync(ct);
		}

		public Task StopAsync(CancellationToken ct = default) {
			return container.StopAsync(ct);
		}

		public Task PauseAsync(CancellationToken ct = default) {
			return container.PauseAsync(ct);
		}

		public Task UnpauseAsync(CancellationToken ct = default) {
			return container.UnpauseAsync(ct);
		}
	}

	class SqlServerContainerBuilder
	{
		public const int DefaultPort = 1433;

		public SqlServerContainer Build() {
			return new SqlServerContainer(new ContainerBuilder()
				.WithImage("mcr.microsoft.com/mssql/server:2019-latest")
				.WithPortBinding(DefaultPort, assignRandomHostPort: true)
				.WithEnvironment("ACCEPT_EULA", "Y")
				.WithEnvironment("MSSQL_SA_PASSWORD", "Pa55w0rd!")
				.WithWaitStrategy(
					Wait.ForUnixContainer()
					.UntilMessageIsLogged("SQL Server is now ready for client connections.")
					.AddCustomWaitStrategy(new WaitUntilSqlConnectionOk()))
				.Build());
		}

		sealed class WaitUntilSqlConnectionOk : IWaitUntil
		{
			public async Task<bool> UntilAsync(IContainer container) {
				var cs = new TestDbConfig {
					Port = container.GetMappedPublicPort(DefaultPort),
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
		static readonly SqlServerContainer sqlContainer;
		readonly SqlServerTestDb testDb;
		public SqlConnection Connection { get; private set; }
		public string ConnectionString => testDb.ConnectionString;
		public readonly TestDbConfig Config;

		public SqlConnection Open() {
			var c = new SqlConnection(ConnectionString);
			c.Open();
			return c;
		}

		static SqlServerFixture() {
			sqlContainer = new SqlServerContainerBuilder().Build();
		}

		public SqlServerFixture() {
			lock (sqlContainer) {
				if (sqlContainer.State != TestcontainersStates.Running)
					sqlContainer.StartAsync().Wait();
			}
			Config = new TestDbConfig {
				Username = sqlContainer.Username,
				Password = sqlContainer.Password,
				Port = sqlContainer.Port,
			};
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
