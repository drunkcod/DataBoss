using Cone;
using DataBoss.Schema;
using System;
using System.ComponentModel.DataAnnotations;

namespace DataBoss.Specs
{
	[Describe(typeof(DataBossScripter))]
	public class DataBossScripterSpec
	{
		[DisplayAs("{0} maps to db type {1}")
		,Row(typeof(DateTime?), "datetime")
		,Row(typeof(DateTime), "datetime not null")
		,Row(typeof(int?), "int")
		,Row(typeof(int), "int not null")
		,Row(typeof(long?), "bigint")
		,Row(typeof(long), "bigint not null")
		,Row(typeof(float?), "real")
		,Row(typeof(float), "real not null")
		,Row(typeof(double?), "float")
		,Row(typeof(double), "float not null")
		,Row(typeof(string), "varchar(max)")
		,Row(typeof(bool), "bit not null")
		,Row(typeof(bool?), "bit")]
		public void to_db_type(Type type, string dbType) {
			Check.That(() => DataBossScripter.ToDbType(type, new StubAttributeProvider()) == dbType);
		}

		public void RequiredAttribute_string_is_not_null() {
			Check.That(() => DataBossScripter.ToDbType(typeof(string), new StubAttributeProvider().Add(new RequiredAttribute())) == "varchar(max) not null");
		}

		public void MaxLengthAttribute_controls_string_column_widht() {
			Check.That(() => DataBossScripter.ToDbType(typeof(string), new StubAttributeProvider().Add(new MaxLengthAttribute(31))) == "varchar(31)");
		}

		public void can_script_select() {
			var scripter = new DataBossScripter();
			Check.That(() => scripter.Select(typeof(DataBossMigrationInfo), typeof(DataBossHistory)) == "select Id, Context, Name from [dbo].[__DataBossHistory]");
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
	[User] varchar(max),
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
