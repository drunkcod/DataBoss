#if MSSQLCLIENT
namespace DataBoss.Data.MsSql
{
	using Microsoft.Data.SqlClient;
#else
namespace DataBoss.Data
{
	using System.Data.SqlClient;
#endif
	using System.Data;

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
	}
}