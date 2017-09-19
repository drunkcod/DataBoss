using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using DataBoss.Data;
using DataBoss.Diagnostics;

namespace DataBoss
{
	struct IdRow { public int Id; }
	
	public static class SqlConnectionExtensions
	{
		public static SqlCommand CreateCommand(this SqlConnection connection, string cmdText) => 
			new SqlCommand(cmdText, connection);

		public static SqlCommand CreateCommand<T>(this SqlConnection connection, string cmdText, T args) {
			var cmd = CreateCommand(connection, cmdText);
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

		public static int ExecuteNonQuery(this SqlConnection connection, SqlTransaction transaction, string cmdText) {
			using (var q = connection.CreateCommand(cmdText)) {
				q.Transaction = transaction;
				return q.ExecuteNonQuery();
			}
		}

		public static int ExecuteNonQuery<T>(this SqlConnection connection, string cmdText, T args) {
			using(var q = CreateCommand(connection, cmdText, args))
				return q.ExecuteNonQuery();
		}

		public static void Into<T>(this SqlConnection connection, string destinationTable, IEnumerable<T> rows) =>
			Into(connection, destinationTable, SequenceDataReader.Create(rows, x => x.MapAll()));

		public static void Into(this SqlConnection connection, string destinationTable, IDataReader toInsert) {
			var scripter = new DataBossScripter();
			connection.ExecuteNonQuery(scripter.ScriptTable(destinationTable, toInsert));
			connection.Insert(destinationTable, toInsert);
		}

		public static void Insert<T>(this SqlConnection connection, string destinationTable, IEnumerable<T> rows) =>
			Insert(connection, null, destinationTable, rows);

		public static void Insert<T>(this SqlConnection connection, SqlTransaction transaction, string destinationTable, IEnumerable<T> rows) {
			new DataBossBulkCopy(connection, transaction).Insert(destinationTable, rows);
		}

		public static void Insert(this SqlConnection connection, string destinationTable, IDataReader toInsert) =>
			Insert(connection, null, destinationTable, toInsert);

		public static void Insert(this SqlConnection connection, SqlTransaction transaction, string destinationTable, IDataReader toInsert) =>
			new DataBossBulkCopy(connection, transaction).Insert(destinationTable, toInsert);

		public static ICollection<int> InsertAndGetIdentities<T>(this SqlConnection connection, string destinationTable, IEnumerable<T> rows) =>
			InsertAndGetIdentities(connection, null, destinationTable, rows);

		public static ICollection<int> InsertAndGetIdentities<T>(this SqlConnection connection, SqlTransaction transaction, string destinationTable, IEnumerable<T> rows) =>
			new DataBossBulkCopy(connection, transaction).InsertAndGetIdentities(destinationTable, rows);

		public static void WithCommand(this SqlConnection connection, Action<SqlCommand> useCommand) {
			using(var cmd = connection.CreateCommand())
				useCommand(cmd);
		}

		public static DatabaseInfo GetDatabaseInfo(this SqlConnection connection) {
			var reader = new DbObjectReader(connection);
			return reader.Single<DatabaseInfo>(@"
				select 
					ServerName = cast(serverproperty('ServerName') as nvarchar(max)),
					ServerVersion = cast(serverproperty('ProductVersion') as nvarchar(max)),
					DatabaseName = db.name,
					DatabaseId = db.database_id,
					CompatibilityLevel = db.compatibility_level
				from sys.databases db where database_id = db_id()");
		}
	}
}