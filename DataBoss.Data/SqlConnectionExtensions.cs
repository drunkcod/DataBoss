using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using DataBoss.Data.Scripting;

namespace DataBoss.Data
{	
	public static class DbConnectionExtensions
	{
		public static IDbCommand CreateCommand(this IDbConnection connection, string commandText) {
			var c = connection.CreateCommand();
			c.CommandText = commandText;
			return c;
		}

		public static object ExecuteScalar(this IDbConnection connection, string commandText) {
			using(var c = connection.CreateCommand(commandText))
				return c.ExecuteScalar();
		}

		public static int ExecuteNonQuery(this IDbConnection connection, string commandText) {
			using(var c = connection.CreateCommand(commandText))
				return c.ExecuteNonQuery();
		}

		public static void Into<T>(this IDbConnection connection, string destinationTable, IEnumerable<T> rows) =>
			Into(connection, destinationTable, rows, new DataBossBulkCopySettings());
		
		public static void Into<T>(this IDbConnection connection, string destinationTable, IEnumerable<T> rows, DataBossBulkCopySettings settings) {
			switch(connection) {
				case SqlConnection x: 
					x.Into(destinationTable, rows, settings);
					break;
				case ProfiledSqlConnection x: 
					x.Into(destinationTable, rows, settings);
					break;
				default: throw new NotSupportedException();
			}
		}

	}

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
			Into(connection, destinationTable, rows, new DataBossBulkCopySettings());

		public static void Into<T>(this SqlConnection connection, string destinationTable, IEnumerable<T> rows, DataBossBulkCopySettings settings) =>
			Into(connection, destinationTable, SequenceDataReader.Create(rows, x => x.MapAll()), settings);

		public static void Into(this SqlConnection connection, string destinationTable, IDataReader toInsert) =>
			Into(connection, destinationTable, toInsert, new DataBossBulkCopySettings());

		public static void Into(this SqlConnection connection, string destinationTable, IDataReader toInsert, DataBossBulkCopySettings settings) {
			var scripter = new DataBossScripter();
			connection.ExecuteNonQuery(scripter.ScriptTable(destinationTable, toInsert));
			connection.Insert(destinationTable, toInsert, settings);
		}

		public static void Insert<T>(this SqlConnection connection, string destinationTable, IEnumerable<T> rows) =>
			Insert(connection, null, destinationTable, rows);

		public static void Insert<T>(this SqlConnection connection, SqlTransaction transaction, string destinationTable, IEnumerable<T> rows) =>
			Insert(connection, transaction, destinationTable, rows, new DataBossBulkCopySettings());

		public static void Insert<T>(this SqlConnection connection, string destinationTable, IEnumerable<T> rows, DataBossBulkCopySettings settings) =>
			connection.Insert(null, destinationTable, rows, settings);

		public static void Insert<T>(this SqlConnection connection, SqlTransaction transaction, string destinationTable, IEnumerable<T> rows, DataBossBulkCopySettings settings) =>
			new DataBossBulkCopy(connection, transaction, settings).Insert(destinationTable, rows);
		
		public static void Insert(this SqlConnection connection, string destinationTable, IDataReader toInsert) =>
			Insert(connection, null, destinationTable, toInsert);

		public static void Insert(this SqlConnection connection, SqlTransaction transaction, string destinationTable, IDataReader toInsert) =>
			Insert(connection, transaction, destinationTable, toInsert, new DataBossBulkCopySettings());

		public static void Insert(this SqlConnection connection, string destinationTable, IDataReader toInsert, DataBossBulkCopySettings settings) =>
			Insert(connection, null, destinationTable, toInsert, settings);

		public static void Insert(this SqlConnection connection, SqlTransaction transaction, string destinationTable, IDataReader toInsert, DataBossBulkCopySettings settings) =>
			new DataBossBulkCopy(connection, transaction, settings).Insert(destinationTable, toInsert);

		public static ICollection<int> InsertAndGetIdentities<T>(this SqlConnection connection, string destinationTable, IEnumerable<T> rows) =>
			InsertAndGetIdentities(connection, null, destinationTable, rows);

		public static ICollection<int> InsertAndGetIdentities<T>(this SqlConnection connection, SqlTransaction transaction, string destinationTable, IEnumerable<T> rows) =>
			new DataBossBulkCopy(connection, transaction).InsertAndGetIdentities(destinationTable, rows);

		public static void WithCommand(this SqlConnection connection, Action<SqlCommand> useCommand) {
			using(var cmd = connection.CreateCommand())
				useCommand(cmd);
		}
	}
}