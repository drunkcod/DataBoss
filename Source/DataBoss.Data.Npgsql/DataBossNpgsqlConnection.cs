using System.Data;
using DataBoss.Linq;
using Npgsql;

namespace DataBoss.Data.Npgsql
{
	public class DataBossNpgsqlConnection : IDataBossConnection
	{
		readonly NpgsqlConnection connection;

		public DataBossNpgsqlConnection(NpgsqlConnection connection) { this.connection = connection; }

		public ISqlDialect Dialect => NpgsqlDialect.Instance;

		public ConnectionState State => connection.State;

		public IDbTransaction BeginTransaction() => connection.BeginTransaction();

		public IDbCommand CreateCommand() => connection.CreateCommand();

		public IDbCommand CreateCommand(string cmdText) => new NpgsqlCommand(cmdText, connection);

		public IDbCommand CreateCommand<T>(string cmdText, T args) {
			var cmd = new NpgsqlCommand(cmdText, connection);
			NpgsqlDialect.AddParameters(cmd, args);
			return cmd;
		}

		public IDbCommand CreateCommand(string cmdText, object args) {
			var cmd = new NpgsqlCommand(cmdText, connection);
			NpgsqlDialect.AddParameters(cmd, args);
			return cmd;
		}

		public void CreateTable(string destinationTable, IDataReader data) {
			using var cmd = CreateCommand(NpgsqlDialect.Scripter.ScriptTable(destinationTable, data));
			cmd.ExecuteNonQuery();
		}

		public void Dispose() => connection.Dispose();

		public void Insert(string destinationTable, IDataReader rows, DataBossBulkCopySettings settings) {
			var columns = string.Join(',', Collection.ArrayInit(rows.FieldCount, x => rows.GetName(x)));
			using var writer = connection.BeginBinaryImport($"COPY {destinationTable}({columns}) FROM STDIN(FORMAT BINARY)");
			for(var row = new object[rows.FieldCount]; rows.Read();) {
				rows.GetValues(row);
				writer.WriteRow(row);
			}
			writer.Complete();
		}

		public void Open() => connection.Open();
	}
}
