using Cone;
using DataBoss.Data;
using DataBoss.Migrations;
using DataBoss.Schema;

namespace DataBoss.Specs
{
	[Describe(typeof(DataBossScripter))]
	public class DataBossScripterSpec
	{
		public void can_script_select() {
			var scripter = new DataBossScripter();
			Check.That(() => scripter.Select(typeof(DataBossMigrationInfo), typeof(DataBossHistory)) == "select Id, Context, Name from [dbo].[__DataBossHistory]");
		}

		public void can_script_reader_as_table() { 
			var scripter = new DataBossScripter();
			var data = SequenceDataReader.Create(new []{ new { Id = 1, Value = "Hello" } }, x => x.MapAll());
			Check.That(() => scripter.ScriptTable("#Hello", data) ==
@"create table [#Hello](
	[Id] int not null,
	[Value] varchar(max)
)");
		}

		public void can_script_history_table() {
			var scripter = new DataBossScripter();

			Check.That(() => scripter.ScriptTable(typeof(DataBossHistory)) == 
@"create table [dbo].[__DataBossHistory](
	[Id] bigint not null,
	[Context] varchar(64) not null,
	[Name] varchar(max) not null,
	[StartedAt] datetime not null,
	[FinishedAt] datetime,
	[User] varchar(max)
)");
		}
		public void can_script_history_table_primary_key_constraint() {
			var scripter = new DataBossScripter();

			Check.That(() => scripter.ScriptConstraints(typeof(DataBossHistory)) == 
@"create clustered index IX___DataBossHistory_StartedAt on [__DataBossHistory](StartedAt)

alter table [__DataBossHistory]
add constraint PK___DataBossHistory primary key(Id,Context)");
		}
	}
}
