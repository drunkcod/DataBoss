#if MSSQLCLIENT
using DataBoss.Data.MsSql;
using Microsoft.Data.SqlClient;
#else
	using System.Data.SqlClient;
#endif

using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Data;
using System.Linq;
using System.Runtime.ExceptionServices;
using DataBoss.Data;
using DataBoss.Linq;

namespace DataBoss.Diagnostics
{
	public class SqlServerMaintenancePlanWizard
	{
		public bool UpdateStatistics = true;
		public double FragmentationThreshold = 5.0;
		public double RebuildThreshold = 30.0;
		public int PageCountThreshold = 1000;

		#pragma warning disable CS0649
		class IndexStatsRow
		{
			[Required]
			public string SchemaName;
			[Required]
			public string TableName;
			[Required]
			public string IndexName;
			[Required]
			public double AvgFragmentationInPercent;
			[Required]
			public long PageCount;
		}
		#pragma warning restore CS0649

		public event EventHandler<SqlServerMaintenancePlanWizardErrorEventArgs> OnError;

		public IEnumerable<SqlServerMaintenancePlan> MakePlans(Func<SqlConnection> getServer, string[] dbs) {
			using(var server = getServer())
				foreach(var item in MakePlans(server, dbs))
					yield return item;
		}

		public IEnumerable<SqlServerMaintenancePlan> MakePlans(SqlConnection server, string[] dbs) {
			var serverName = new SqlConnectionStringBuilder(server.ConnectionString).DataSource;

			foreach (var database in dbs) {
				if(server.State != ConnectionState.Open)
					server.Open();
				server.ChangeDatabase(database);
				if(TryCreateMaintenancePlan(serverName, database, server, out var result))
					yield return result.Key;
				else if(OnError != null)
					OnError(this, new SqlServerMaintenancePlanWizardErrorEventArgs(server, result.Value));
				else ExceptionDispatchInfo.Capture(result.Value).Throw();
			}
		}

		bool TryCreateMaintenancePlan(string serverName, string database, SqlConnection db, out KeyValuePair<SqlServerMaintenancePlan, Exception> result) {
			try {
				result = KeyValuePair.Create(new SqlServerMaintenancePlan(serverName, database, GetMaintenanceCommands(db).ToArray()), (Exception)null);
				return true;
			}
			catch (Exception ex) {
				result = KeyValuePair.Create((SqlServerMaintenancePlan)null, ex);
				return false;
			}
		}

		IEnumerable<string> GetMaintenanceCommands(SqlConnection db) {
			if(UpdateStatistics)
				yield return "exec sp_updatestats";

			var toProcess = db.Query<IndexStatsRow>($@"
				declare @db_id int
				set @db_id = db_id()
			
				select
					database_id,
					{nameof(IndexStatsRow.SchemaName)} = schema_name(o.schema_id),
					{nameof(IndexStatsRow.TableName)} = o.name,
					{nameof(IndexStatsRow.IndexName)} = b.name, 
					{nameof(IndexStatsRow.AvgFragmentationInPercent)} = a.avg_fragmentation_in_percent,
					{nameof(IndexStatsRow.PageCount)} = a.page_count
				from sys.dm_db_index_physical_stats(@db_id, null, null, null, 'sampled') a
				join sys.indexes b on a.object_id = b.object_id and a.index_id = b.index_id
				join sys.objects o on o.object_id = a.object_id
				where b.name is not null
				and avg_fragmentation_in_percent >= @FragmentationThreshold
				and page_count > @PageCountThreshold", new DataBossQueryOptions {
					Parameters = new {
						FragmentationThreshold,
						PageCountThreshold,
					},
					CommandTimeout = 0,
			});
			foreach (var item in toProcess) {
				var action = item.AvgFragmentationInPercent >= RebuildThreshold ? "rebuild" : "reorganize";
				yield return $"alter index [{item.IndexName}] on [{item.SchemaName}].[{item.TableName}] {action}";
			}
		}
	}
}
