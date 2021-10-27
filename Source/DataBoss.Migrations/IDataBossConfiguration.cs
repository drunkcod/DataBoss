namespace DataBoss
{
	public interface IDataBossConfiguration
	{
		string GetConnectionString();
		IDataBossMigration GetTargetMigration();
		string Script { get; }

	}
}
