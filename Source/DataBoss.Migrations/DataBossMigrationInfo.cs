namespace DataBoss.Migrations
{
	public class DataBossMigrationInfo
	{
		string context;

		public long Id;
		public string Context {
			get => context ?? string.Empty;
			set { context = value; }
		}
		public string Name;
		public byte[] MigrationHash;

		public string FullId => string.IsNullOrEmpty(Context) ? Id.ToString() : $"{Context}.{Id}";
	}
}