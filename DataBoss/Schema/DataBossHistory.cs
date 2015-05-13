using System.Data.Linq.Mapping;

namespace DataBoss.Schema
{
	[Table(Name = "__DataBossHistory")]
	public class DataBossHistory
	{
		[Column(IsPrimaryKey = true)]
		public long Id;

	}
}
