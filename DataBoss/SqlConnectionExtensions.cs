using System.Data.SqlClient;
using DataBoss.Data;
using DataBoss.Diagnostics;
using System;
using System.Collections.Generic;
using System.Linq;

namespace DataBoss
{
	struct IdRow { public int Id; }

	public static class SqlConnectionExtensions
	{
		const string TempTableName = "#$";

		public static SqlCommand CreateCommand(this SqlConnection connection, string cmdText) {
			return new SqlCommand(cmdText, connection);
		}

		public static SqlCommand CreateCommand<T>(this SqlConnection connection, string cmdText, T args) {
			var cmd = new SqlCommand(cmdText, connection);
			ToParams.AddTo(cmd, args);
			return cmd;
		}

		public static object ExecuteScalar(this SqlConnection connection, string cmdText) {
			using(var q = connection.CreateCommand(cmdText))
				return q.ExecuteScalar();
		}

		public static object ExecuteScalar<T>(this SqlConnection connection, string cmdText, T args) {
			using(var q = CreateCommand(connection, cmdText, args))
				return q.ExecuteScalar();
		}

		public static int ExecuteNonQuery(this SqlConnection connection, string cmdText) {
			using(var q = connection.CreateCommand(cmdText))
				return q.ExecuteNonQuery();
		}

		public static int ExecuteNonQuery<T>(this SqlConnection connection, string cmdText, T args) {
			using(var q = CreateCommand(connection, cmdText, args))
				return q.ExecuteNonQuery();
		}

		public static ICollection<int> InsertAndGetIdentities<T>(this SqlConnection connection, string destinationTable, IEnumerable<T> rows) {
			var n = 0;
			var toInsert = SequenceDataReader.Create(rows, x => {
				x.Map("$", _ => ++n);
				x.MapAll();
			});
			var scripter = new DataBossScripter();
			connection.ExecuteNonQuery($@"
				{scripter.ScriptTable(TempTableName, toInsert)}
				create clustered index [#$_$] on {TempTableName}([{toInsert.GetName(0)}])
			");

			using(var bulkCopy = new SqlBulkCopy(connection) { DestinationTableName = TempTableName })
				bulkCopy.WriteToServer(toInsert);
			
			var columns = string.Join(",", Enumerable.Range(1, toInsert.FieldCount - 1).Select(toInsert.GetName));
			var reader = new DbObjectReader(connection) {
				CommandTimeout = null,
			};
			return reader.Read<IdRow>($@"
				insert {destinationTable} with(tablock)({columns})
				output inserted.$identity as {nameof(IdRow.Id)}
				select {columns}
				from {TempTableName}
				order by [$]
			
				drop table {TempTableName}
			").OrderBy(x => x.Id).Select(x => x.Id).ToList();
		}

		public static void WithCommand(this SqlConnection connection, Action<SqlCommand> useCommand) {
			using(var cmd = connection.CreateCommand())
				useCommand(cmd);
		}

		public static DatabaseInfo GetDatabaseInfo(this SqlConnection connection)
		{
			var reader = new DbObjectReader(connection);
			return reader.Single<DatabaseInfo>(
				@"select 
						ServerName = serverproperty('ServerName'),
						ServerVersion = serverproperty('ProductVersion'),
						DatabaseName = db.name,
						DatabaseId = db.database_id,
						CompatibilityLevel = db.compatibility_level
					from sys.databases db where database_id = db_id()"
				);
		}
	}
}