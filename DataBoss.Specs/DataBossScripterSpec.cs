using Cone;
using DataBoss.Schema;
using System.Collections.Generic;

namespace DataBoss.Specs
{
	[Describe(typeof(DataBossScripter))]
	public class DataBossScripterSpec
	{
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
