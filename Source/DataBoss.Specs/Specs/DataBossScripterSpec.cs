using System.ComponentModel.DataAnnotations.Schema;
using CheckThat;
using DataBoss.Data;
using DataBoss.Data.MsSql;
using DataBoss.Data.Scripting;
using DataBoss.Schema;
using Xunit;

namespace DataBoss
{
	public class DataBossScripterSpec
	{
		static DataBossScripter NewScripter() => new DataBossScripter(MsSqlDialect.Instance);

		[Fact]
		public void can_script_reader_as_table() {
			var scripter = NewScripter();
			var data = SequenceDataReader.Create(new[] { new { Id = 1, Value = "Hello" } }, x => x.MapAll());
			Check.That(() => scripter.ScriptTable("#Hello", data) == scripter.Join(
				"create table [#Hello](",
				"	[Id] int not null,",
				"	[Value] nvarchar(max)",
				")"));
		}

		[Fact]
		public void script_reader_as_values_table() {
			var scripter = NewScripter();
			var data = SequenceDataReader.Create(new[] { new { Id = 1, Value = "Hello" } }, x => x.MapAll());
			Check.That(() => scripter.ScriptValuesTable("Hello", data) ==
@"(values
  (1, N'Hello')) Hello([Id], [Value])");
		}

		[Fact]
		public void can_script_history_table() {
			var scripter = NewScripter();

			Check.That(() => scripter.ScriptTable(typeof(DataBossHistory)) == scripter.Join(
				"create table [dbo].[__DataBossHistory](",
				"	[Id] bigint not null,",
				"	[Context] varchar(64) not null,",
				"	[Name] varchar(max) not null,",
				"	[StartedAt] datetime not null,",
				"	[FinishedAt] datetime,",
				"	[User] varchar(max)",
				")"));
		}

		[Fact]
		public void can_script_history_table_primary_key_constraint() {
			var scripter = NewScripter();

			Check.That(() => scripter.ScriptConstraints(typeof(DataBossHistory)) == scripter.Join(
				"create clustered index IX___DataBossHistory_StartedAt on [dbo].[__DataBossHistory](StartedAt)",
				"",
				"alter table [dbo].[__DataBossHistory]",
				"add constraint PK___DataBossHistory primary key(Id,Context)"));
		}

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

			[Fact]
			public void puts_ordered_columns_before_non_determined_ones() {
				var scripter = NewScripter();
				Check.That(() => scripter.ScriptTable(typeof(HavingOrderedAndNotColumns)) == scripter.Join(
					"create table [MyTable](",
					"	[Id] int not null,",
					"	[Other] int not null",
					")"));
			}
		}
	}
}
