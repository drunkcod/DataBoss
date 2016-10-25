using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using DataBoss.Data;

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

	public class SqlServerMaintenancePlanWizard
	{
		public bool UpdateStatistics = true;
		public double FragmentationThreshold = 5.0;
		public double RebuildThreshold = 30.0;
		public int PageCountThreshold = 1000;

		class IndexStatsRow
		{
			public string TableName;
			public string IndexName;
			public double AvgFragmentationInPercent;
			public long PageCount;
		}

		public IEnumerable<SqlServerMaintenancePlan> MakePlans(Func<SqlConnection> getServer, string[] dbs) {
			using(var server = getServer()) {
				server.Open();
				foreach(var item in MakePlans(server, dbs))
					yield return item;
			}
		}

		public IEnumerable<SqlServerMaintenancePlan> MakePlans(SqlConnection server, string[] dbs) {
			var serverName = new SqlConnectionStringBuilder(server.ConnectionString).DataSource;
			var reader = new DbObjectReader(server) { CommandTimeout = null };

			foreach (var database in dbs) {
				server.ChangeDatabase(database);
				yield return new SqlServerMaintenancePlan(serverName, database, GetMaintenanceCommands(reader).ToArray());
			}
		}

		IEnumerable<string> GetMaintenanceCommands(DbObjectReader reader) {
			if(UpdateStatistics)
				yield return "exec sp_updatestats";

			var toProcess = reader.Query($@"
				declare @db_id int
				set @db_id = db_id()
			
				select
					database_id,
					{nameof(IndexStatsRow.TableName)} = object_name(a.object_id),
					{nameof(IndexStatsRow.IndexName)} = b.name, 
					{nameof(IndexStatsRow.AvgFragmentationInPercent)} = a.avg_fragmentation_in_percent,
					{nameof(IndexStatsRow.PageCount)} = a.page_count
				from sys.dm_db_index_physical_stats(@db_id, null, null, null, 'sampled') a
				join sys.indexes b on a.object_id = b.object_id and a.index_id = b.index_id
				where b.name is not null
				and avg_fragmentation_in_percent >= @FragmentationThreshold
				and page_count > @PageCountThreshold", new {
				FragmentationThreshold,
				PageCountThreshold,
			}).Read<IndexStatsRow>();
			foreach (var item in toProcess) {
				var action = item.AvgFragmentationInPercent >= RebuildThreshold ? "rebuild" : "reorganize";
				yield return $"alter index [{item.IndexName}] on [{item.TableName}] {action}";
			}
		}
	}
}
