using Cone;
using DataBoss.Schema;
using System;
using System.Reflection;

namespace DataBoss.Specs
{
	class StubAttributeProvider : ICustomAttributeProvider
	{
		public object[] GetCustomAttributes(bool inherit) {
			throw new NotImplementedException();
		}

		public object[] GetCustomAttributes(Type attributeType, bool inherit) {
			return new object[0];;
		}

		public bool IsDefined(Type attributeType, bool inherit) {
			throw new NotImplementedException();
		}
	}

	[Describe(typeof(DataBossScripter))]
	public class DataBossScripterSpec
	{
		[DisplayAs("{0} maps to db type {1}")
		,Row(typeof(DateTime?), "datetime")
		,Row(typeof(DateTime), "datetime not null")
		,Row(typeof(long), "bigint not null")
		,Row(typeof(string), "varchar(max)")]
		public void to_db_type(Type type, string dbType) {
			Check.That(() => DataBossScripter.ToDbType(type, new StubAttributeProvider()) == dbType);
		}

		public void can_script_history_table() {
			var scripter = new DataBossScripter();

			Check.That(() => scripter.Script(typeof(DataBossHistory)) == 
@"create table [__DataBossHistory](
	[Id] bigint not null,
	[Context] varchar(64) not null,
	[Name] varchar(max) not null,
	[StartedAt] datetime not null,
	[FinishedAt] datetime,
	[User] varchar(max),
)");
		}
	}
}
