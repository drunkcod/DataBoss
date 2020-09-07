using System.Data;

namespace DataBoss.Data
{
	public struct DataBossQueryOptions
	{
		public object Parameters;
		public int? CommandTimeout;
		public CommandType CommandType;
		public bool Buffered;
	}
}