#if MSSQLCLIENT
namespace DataBoss.Data.MsSql
{
	using Microsoft.Data.SqlClient;
#else
namespace DataBoss.Data
{
	using System.Data.SqlClient;
#endif
	using System;
	using System.Collections.Generic;
	using System.Data;
	using DataBoss.Data.Scripting;
	using System.Linq.Expressions;
	using DataBoss.Data.SqlServer;
	using System.Reflection;

	public class MsSqlDialect : ISqlDialect
	{
		private MsSqlDialect()  { }

		public static readonly MsSqlDialect Instance = new MsSqlDialect();
		
		public string ParameterPrefix => "@";

		public Expression MakeRowVersionParameter(string name, Expression readMember)=> 
			Expression.MemberInit(
				Expression.New(
					typeof(SqlParameter).GetConstructor(new []{ typeof(string), typeof(SqlDbType), typeof(int) }),
					Expression.Constant(name),
					Expression.Constant(SqlDbType.Binary),
					Expression.Constant(8)), 
					Expression.Bind(
						typeof(SqlParameter).GetProperty(nameof(SqlParameter.Value)),
						Expression.Convert(Expression.Field(readMember, nameof(RowVersion.Value)), typeof(object))));
	}

	public class DataBossSqlConnection : IDataBossConnection
	{
		readonly SqlConnection connection;

		public DataBossSqlConnection(SqlConnection connection) { this.connection = connection; }

		public ISqlDialect Dialect => MsSqlDialect.Instance;

		public void CreateTable(string destinationTable, IDataReader data) =>
			connection.CreateTable(destinationTable, data);

		public void Insert(string destinationTable, IDataReader rows, DataBossBulkCopySettings settings) =>
			connection.Insert(destinationTable, rows, settings);

		public IDbCommand CreateCommand(string cmdText) =>
			new SqlCommand(cmdText, connection);

		public IDbCommand CreateCommand<T>(string cmdText, T args) {
			var cmd = new SqlCommand(cmdText, connection);
			ToParams.AddTo(cmd, args);
			return cmd;
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

		public static void CreateTable(this SqlConnection connection, string tableName, IDataReader data) {
			var scripter = new DataBossScripter();
			connection.ExecuteNonQuery(scripter.ScriptTable(tableName, data));
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
			CreateTable(connection, destinationTable, toInsert);
			Insert(connection, destinationTable, toInsert, settings);
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