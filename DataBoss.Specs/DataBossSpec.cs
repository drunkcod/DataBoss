using System;
using System.Collections.Generic;
using System.Data.Linq;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Cone;
using DataBoss.Schema;
using System.Data.Linq.Mapping;

namespace DataBoss.Specs
{
	[Table(Name = "sys.objects")]
	class SysObjects
	{
		[Column(Name = "name")]
		public string Name;
	}

	[Feature("DataBoss")]
	public class DataBossSpec
	{
		public void rollbacks_failed_migration() {
			var config = new DataBossConfiguration {
				Server = ".",
				Database = "DataBoss Tests",
			};
			using(var db = new SqlConnection(config.GetConnectionString())) {
				var dataBoss = new Program(db);
				var context = new DataContext(db);
				Assume.That(() => !context.GetTable<SysObjects>().Any(x => x.Name == "Foo"));
				
				var migrations = context.GetTable<DataBossHistory>();
				dataBoss.Initialize(config);
				var migrator = new DataBossSqlMigrationScope(db);
				var failingMigration = new DataBossMigrationInfo {
					Id = migrations.Max(x => x.Id) + 1,
					Name = "Failing Migration",
				};

				migrator.Begin(failingMigration);
				migrator.Execute("create table Foo(Id int not null)");
				migrator.Execute("select syntax error");
				migrator.Execute("create table Foo(Id int not null)");
				migrator.Done();

				Check.That(
					() => !migrations.Any(x => x.Id == failingMigration.Id),
					() => !context.GetTable<SysObjects>().Any(x => x.Name == "Foo"));
			}
		}
	}
}
