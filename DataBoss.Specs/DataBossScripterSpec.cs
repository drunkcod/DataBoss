using Cone;
using DataBoss.Schema;
using System;
using System.ComponentModel.DataAnnotations;
using DataBoss.Migrations;
using System.Data;
using DataBoss.Data;
using System.Data.SqlTypes;
using System.ComponentModel.DataAnnotations.Schema;

namespace DataBoss.Specs
{
	[Describe(typeof(DataBossScripter))]
	public class DataBossScripterSpec
	{
		[DisplayAs("{0} maps to db type {1}")
		,Row(typeof(DateTime?), "datetime", true)
		,Row(typeof(DateTime), "datetime", false)
		,Row(typeof(int?), "int", true)
		,Row(typeof(int), "int", false)
		,Row(typeof(long?), "bigint", true)
		,Row(typeof(long), "bigint", false)
		,Row(typeof(float?), "real", true)
		,Row(typeof(float), "real", false)
		,Row(typeof(double?), "float", true)
		,Row(typeof(double), "float", false)
		,Row(typeof(string), "varchar(max)", true)
		,Row(typeof(bool), "bit", false)
		,Row(typeof(bool?), "bit", true)
		,Row(typeof(SqlMoney), "money", false)
		,Row(typeof(SqlMoney?), "money", true)]
		public void to_db_type(Type type, string dbType, bool nullable) =>
			Check.That(() => DataBossScripter.ToDbType(type, new StubAttributeProvider()) == new DataBossDbType(dbType, nullable));
	
		class MyRowType
		{
			[Column(TypeName = "decimal(18, 5)")]
			public decimal Value;
		}
		public void to_db_type_with_column_type_override() {
			var column = typeof(MyRowType).GetField(nameof(MyRowType.Value));
			Check.That(() => DataBossScripter.ToDbType(column.FieldType, column) == new DataBossDbType("decimal(18, 5)", false));
		}

		public void RequiredAttribute_string_is_not_null() {
			Check.That(() => DataBossScripter.ToDbType(typeof(string), new StubAttributeProvider().Add(new RequiredAttribute())) == new DataBossDbType("varchar(max)", false));
		}

		public void MaxLengthAttribute_controls_string_column_widht() {
			Check.That(() => DataBossScripter.ToDbType(typeof(string), new StubAttributeProvider().Add(new MaxLengthAttribute(31))) == new DataBossDbType("varchar(31)", true));
		}

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
