using System.Data;
using DataBoss.Data;

namespace DataBoss
{
	public interface IDataBossConfiguration
	{
		string GetConnectionString();
		IDbConnection GetDbConnection();
		IDataBossMigration GetTargetMigration();
		string Script { get; }
		string DefaultSchema { get; }
	}

	static class DataBossConfigurationExtensions
	{
		public static IDataBossConnection GetConnection(this IDataBossConfiguration self) =>
			DbConnectionExtensions.Wrap(self.GetDbConnection());
	}
}
