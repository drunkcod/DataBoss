namespace DataBoss.Diagnostics
{
	public class SqlServerMaintenancePlan
	{
		public readonly string Server;
		public readonly string Database;
		public readonly string[] Commands;

		public SqlServerMaintenancePlan(string server, string database, string[] commands)
		{
			this.Server = server;
			this.Database = database;
			this.Commands = commands;
		}
	}
}
