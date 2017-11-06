using System.ComponentModel.DataAnnotations.Schema;
using Cone;
using DataBoss.Data;
using DataBoss.Data.Scripting;
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
@"create clustered index IX___DataBossHistory_StartedAt on [dbo].[__DataBossHistory](StartedAt)

alter table [dbo].[__DataBossHistory]
add constraint PK___DataBossHistory primary key(Id,Context)");
		}

		[Context("Type to table")]
		public class DataBossScripterTypeToTableSpec
		{
			#pragma warning disable CS0649
			[Table("MyTable")]
			class HavingOrderedAndNotColumns
			{
				[Column(Order = 1)]
				public int Id;
				[Column]
				public int Other;
			}
			#pragma warning restore CS0649

			public void puts_ordered_columns_before_non_determined_ones() {
				var scripter = new DataBossScripter();
				Check.That(() => scripter.ScriptTable(typeof(HavingOrderedAndNotColumns)) ==
@"create table [MyTable](
	[Id] int not null,
	[Other] int not null
)");
			}
		}
	}
}
