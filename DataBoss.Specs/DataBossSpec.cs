using System;
using Cone;
using DataBoss.Schema;
using System.Data.Linq;
using System.Data.Linq.Mapping;
using System.Data.SqlClient;
using System.Linq;
using DataBoss.SqlServer;

namespace DataBoss.Specs
{
	[Feature("DataBoss")]
	public class DataBossSpec
	{
		SqlConnection Connection;
		Program DataBoss;
		DataContext Context;

		[BeforeEach]
		public void BeforeEach() {
			var config = new DataBossConfiguration {
				ServerInstance = ".",
				Database = "DataBoss Tests",
			};
			Connection = new SqlConnection(config.GetConnectionString());
			DataBoss = new Program(new NullDataBossLog(), Connection);
			Context = new DataContext(Connection, new DataBossMappingSource());
			DataBoss.Initialize(config);
		}

		[AfterEach]
		public void Cleanup() {
			DataBoss = null;
			Context.Dispose();
			Context = null;
			Connection.Dispose();
			Connection = null;
		}

		IQueryable<SysObjects> SysObjects => Context.GetTable<SysObjects>();
		IQueryable<DataBossHistory> Migrations => Context.GetTable<DataBossHistory>();

		public void rollbacks_failed_migration() {
			Assume.That(() => !SysObjects.Any(x => x.Name == "Foo"));
				
			var failingMigration = new DataBossMigrationInfo {
				Id = Migrations.Max(x => (long?)x.Id).GetValueOrDefault() + 1,
				Name = "Failing Migration",
			};

			Apply(failingMigration, migrator => {
				migrator.Execute("create table Foo(Id int not null)");//should work
				migrator.Execute("select syntax error");//should error
				migrator.Execute("create table Foo(Id int not null)");//should be ignored
			});

			Check.That(
				() => !Migrations.Any(x => x.Id == failingMigration.Id),
				() => !SysObjects.Any(x => x.Name == "Foo"));
		}

		public void happy_path_is_happ() {

			if(Context.GetTable<SysObjects>().Any(x => x.Name == "Bar"))
				using(var cmd = new SqlCommand("drop table Bar", Connection))
					cmd.ExecuteNonQuery();
				
			var migration = new DataBossMigrationInfo {
				Id = Migrations.Max(x => (long?)x.Id).GetValueOrDefault() + 1,
				Name = "Great Success!",
			};

			Apply(migration, migrator => {
				migrator.Execute("create table Bar(Id int not null)");
			});

			Check.That(() => SysObjects.Any(x => x.Name == "Bar"));
		}

		void Apply(DataBossMigrationInfo info, Action<IDataBossMigrationScope> scope) {
			var migrator = new DataBossSqlMigrationScope(Connection);
			migrator.Begin(info);
			scope(migrator);
			migrator.Done();
		}
	}
}
