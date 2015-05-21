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
		public void DateTime_is_db_datetime() {
			Check.That(() => DataBossScripter.ToDbType(typeof(DateTime), new StubAttributeProvider()) == "datetime not null");
		}

		public void nullable_DateTime_is_db_datetime_not_null() {
			Check.That(() => DataBossScripter.ToDbType(typeof(DateTime?), new StubAttributeProvider()) == "datetime");
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
