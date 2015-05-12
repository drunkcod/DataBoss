namespace DataBoss
{
	public class DataBossMigrationInfo
	{
		public long Id;
		public string Context;
		public string Name;

		public string FullId { get { 
			return string.IsNullOrEmpty(Context) ? Id.ToString() : string.Format("{0}.{1}", Context, Id); 
		} }
	}
}