#if MSSQLCLIENT
namespace DataBoss.Data.MsSql
{
	using Microsoft.Data.SqlClient;
#else
namespace DataBoss.Data.SqlClient
{
	using System.Data.SqlClient;
#endif
	using System.Data;
	using System;
	using System.Threading.Tasks;
	using System.Data.Common;
	using System.Threading;

	public sealed class DataBossSqlConnection : IDataBossConnection
	{
		readonly SqlConnection connection;

		public DataBossSqlConnection(SqlConnection connection) { this.connection = connection; }

		public ISqlDialect Dialect => MsSqlDialect.Instance;

		public ConnectionState State => connection.State;

		public void Dispose() => connection.Dispose();

		public void Open() => connection.Open();

		public IDbTransaction BeginTransaction() => connection.BeginTransaction();

		public void CreateTable(string destinationTable, IDataReader data) =>
			connection.CreateTable(destinationTable, data);

		public void Insert(string destinationTable, IDataReader rows, DataBossBulkCopySettings settings) =>
			connection.Insert(destinationTable, rows, settings);

		public Task InsertAsync(string destinationTable, DbDataReader rows, DataBossBulkCopySettings settings, CancellationToken cancellationToken = default) =>
			new DataBossBulkCopy(connection, null, settings).InsertAsync(destinationTable, rows, cancellationToken);

		public IDbCommand CreateCommand() =>
			new SqlCommand { Connection = connection };

		public IDbCommand CreateCommand(string cmdText) =>
			new SqlCommand(cmdText, connection);

		public IDbCommand CreateCommand<T>(string cmdText, T args) {
			var cmd = new SqlCommand(cmdText, connection);
			cmd.AddParameters(args);
			return cmd;
		}

		public IDbCommand CreateCommand(string cmdText, object args) {
			var cmd = new SqlCommand(cmdText, connection);
			cmd.AddParameters(args);
			return cmd;
		}

		public void EnsureDatabase() {
			var cs = new SqlConnectionStringBuilder(connection.ConnectionString);
			var dbName = cs.InitialCatalog;
			cs.InitialCatalog = string.Empty;
			using var c = new SqlConnection(cs.ToString());
			using var cmd = c.CreateCommand();
			c.Open();
			var dbToCreate = cmd.ExecuteScalar("select case when db_id(@db) is null then quotename(@db) else null end", new { db = dbName });
			if (dbToCreate is DBNull)
				return;
			cmd.ExecuteNonQuery($"create database {dbToCreate}");
		}

		public int GetTableVersion(string tableName) {
			using var c = CreateCommand(
				  "with table_version(table_name, version) as (\n"
				+ "select table_name = tables.name, version = isnull((\n"
				+ "select cast(value as int)\n"
				+ "from sys.extended_properties p\n"
				+ "where p.name = 'version' and p.class = 1 and tables.object_id = p.major_id\n"
				+ "), 1)\n"
				+ "from sys.tables\n"
				+ ")\n"
				+ "select isnull((\n"
				+ "select version\n"
				+ "from table_version\n"
				+ "where table_name = @tableName), 0)", new { tableName });
			return (int)c.ExecuteScalar();
		}

		public void SetTableVersion(string tableName, int version) {
			var addOrUpdate = HasTableVersion(tableName) == 0 ? "sp_addextendedproperty" : "sp_updateextendedproperty";
			using var c = CreateCommand(addOrUpdate, new {
				name = "version",
				value = version,
				level0type = "Schema",
				level0name = "dbo",
				level1type = "Table",
				level1name = "__DataBossHistory",
			});
			c.CommandType = CommandType.StoredProcedure;
			c.ExecuteNonQuery();
		}

		int HasTableVersion(string tableName) {
			using var c = CreateCommand(
				"select case when (\n"
				+ "select cast(value as int)\n"
				+ "from sys.extended_properties p\n"
				+ "where p.name = 'version' and p.class = 1 and tables.object_id = p.major_id\n"
				+ ") is null then 0 else 1 end\n"
				+ "from sys.tables\n"
				+ "where name = @tableName", new { tableName });
			return (int)c.ExecuteScalar();
		}

		public string GetDefaultSchema() {
			using var c = CreateCommand(
				  "select isnull(default_schema_name, 'dbo')\n"
				+ "from sys.database_principals\n"
				+ "where principal_id = database_principal_id()");

			return (string)c.ExecuteScalar();
		}

	}
}