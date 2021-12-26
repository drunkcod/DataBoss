using System;
using System.Data;
using System.Linq;
using System.Linq.Expressions;
using DataBoss.Data.Support;
using Npgsql;

namespace DataBoss.Data.Npgsql
{
	public class NpgsqlDialect : SqlDialect<NpgsqlDialect, NpgsqlCommand>, ISqlDialect
	{
		public string ParameterPrefix => string.Empty;

		public string FormatName(string columnName) => columnName;
		
		public string GetTypeName(DataBossDbType dbType) => dbType.ToString();

		public bool TryCreateDialectSpecificParameter(string name, Expression readMember, out Expression? create) {
			create = default;
			return false;
		}
	}

	public class DataBossNpgsqlConnection : IDataBossConnection
	{
		readonly NpgsqlConnection connection;

		public DataBossNpgsqlConnection(NpgsqlConnection connection) { this.connection = connection; }

		public ISqlDialect Dialect => NpgsqlDialect.Instance;

		public ConnectionState State => connection.State;

		public IDbTransaction BeginTransaction(string transactionName) => throw new NotSupportedException();

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
			var columns = string.Join(',', Enumerable.Range(0, rows.FieldCount).Select(x => rows.GetName(x)));
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
