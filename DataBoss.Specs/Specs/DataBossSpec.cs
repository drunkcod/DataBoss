using System;
using System.Data.Linq;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using Cone;
using DataBoss.Migrations;
using DataBoss.Schema;
using DataBoss.SqlServer;
using DataBoss.Testing;
using Xunit;

namespace DataBoss.Specs
{
	public class DataBossTestsFixture 
	{
		public string DatabaseName;

		public DataBossTestsFixture() {
			DatabaseName = DatabaseSetup.GetTemporaryInstance("DataBoss Tests").InitialCatalog;
			DatabaseSetup.RegisterForAutoCleanup();
		}
	}

	[Feature("DataBoss")]
	public class DataBossSpec : IClassFixture<DataBossTestsFixture>, IDisposable
	{
		SqlConnection Connection;
		DataBossShellExecute ShellExecute;
		string ShellExecuteOutput;
		DataBoss DataBoss;
		DataContext Context;

		public DataBossSpec(DataBossTestsFixture db) {
			var config = new DataBossConfiguration {
				ServerInstance = ".",
				Database = db.DatabaseName,
			};
			Connection = new SqlConnection(config.GetConnectionString());
			Connection.Open();
			ShellExecuteOutput = string.Empty;
			ShellExecute = new DataBossShellExecute(Encoding.Unicode);
			ShellExecute.OutputDataReceived += (_, e) => ShellExecuteOutput += e.Data; 
			DataBoss = DataBoss.Create(config, new NullDataBossLog());
			Context = new DataContext(Connection, new DataBossMappingSource());
			DataBoss.Initialize();
		}

		void IDisposable.Dispose() {
			DataBoss = null;
			Context.Dispose();
			Context = null;
			Connection.Dispose();
			Connection = null;
		}

		IQueryable<SysObjects> SysObjects => Context.GetTable<SysObjects>();
		IQueryable<DataBossHistory> Migrations => Context.GetTable<DataBossHistory>();

		[Fact]
		public void rollbacks_failed_migration() {
			Assume.That(() => !SysObjects.Any(x => x.Name == "Foo"));
				
			var failingMigration = new DataBossMigrationInfo {
				Id = Migrations.Max(x => (long?)x.Id).GetValueOrDefault() + 1,
				Name = "Failing Migration",
			};

			Apply(failingMigration, migrator => {
				migrator.Execute(DataBossQueryBatch.Query("create table Foo(Id int not null)", string.Empty));//should work
				migrator.Execute(DataBossQueryBatch.Query("select syntax error", string.Empty));//should error
				migrator.Execute(DataBossQueryBatch.Query("create table Foo(Id int not null)", string.Empty));//should be ignored
			});

			Check.That(
				() => !Migrations.Any(x => x.Id == failingMigration.Id),
				() => !SysObjects.Any(x => x.Name == "Foo"));
		}

		[Fact]
		public void happy_path_is_happy() {
			if(Context.GetTable<SysObjects>().Any(x => x.Name == "Bar"))
				using(var cmd = new SqlCommand("drop table Bar", Connection))
					cmd.ExecuteNonQuery();
				
			var migration = new DataBossMigrationInfo {
				Id = Migrations.Max(x => (long?)x.Id).GetValueOrDefault() + 1,
				Name = "Great Success!",
			};

			Apply(migration, migrator => {
				migrator.Execute(DataBossQueryBatch.Query("create table Bar(Id int not null)", string.Empty));
			});

			Check.That(() => SysObjects.Any(x => x.Name == "Bar"));
		}

		[Fact]
		public void external_command_gets_connection_string_in_environment_variable() {
			var migration = new DataBossMigrationInfo {
				Id = Migrations.Max(x => (long?)x.Id).GetValueOrDefault() + 1,
				Name = "External Command",
			};
			Apply(migration, migrator => {
				migrator.Execute(DataBossQueryBatch.ExternalCommand("cmd /U /C echo %DATABOSS_CONNECTION%", string.Empty));
			});
			Check.That(() => ShellExecuteOutput == Connection.ConnectionString);
		}

		void Apply(DataBossMigrationInfo info, Action<IDataBossMigrationScope> scope) {
			var migrator = new DataBossMigrationScope(
				DataBossMigrationScopeContext.From(Connection),
				Connection, 
				ShellExecute);

			migrator.Begin(info);
			scope(migrator);
			migrator.Done();
		}
	}
}
