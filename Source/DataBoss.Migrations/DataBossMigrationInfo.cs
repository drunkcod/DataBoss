namespace DataBoss.Migrations
{
	public class DataBossMigrationInfo
	{
		public long Id;
		public string Context;
		public string Name;

		public string FullId => string.IsNullOrEmpty(Context) ? Id.ToString() : $"{Context}.{Id}";
	}
}