using System;
using System.Data;
using System.Linq.Expressions;
using DataBoss.Data.Support;
using Npgsql;

namespace DataBoss.Data.Npgsql
{
	public class NpsqlDialect : SqlDialect<NpsqlDialect, NpgsqlCommand>, ISqlDialect
	{
		public string ParameterPrefix => string.Empty;
		public string GetTypeName(DataBossDbType dbType) {
			throw new NotImplementedException();
		}

		public bool TryCreateDialectSpecificParameter(string name, Expression readMember, out Expression? create) {
			create = default;
			return false;
		}
	}

	public class DataBossNpgsqlConnection : IDataBossConnection
	{
		readonly NpgsqlConnection connection;

		public DataBossNpgsqlConnection(NpgsqlConnection connection) { this.connection = connection; }

		public ISqlDialect Dialect => NpsqlDialect.Instance;

		public ConnectionState State => connection.State;

		public IDbTransaction BeginTransaction(string transactionName) {
			throw new NotImplementedException();
		}

		public IDbCommand CreateCommand() => connection.CreateCommand();

		public IDbCommand CreateCommand(string cmdText) => new NpgsqlCommand(cmdText, connection);

		public IDbCommand CreateCommand<T>(string cmdText, T args) {
			var cmd = new NpgsqlCommand(cmdText, connection);
			NpsqlDialect.AddParameters(cmd, args);
			return cmd;
		}

		public IDbCommand CreateCommand(string cmdText, object args) {
			var cmd = new NpgsqlCommand(cmdText, connection);
			NpsqlDialect.AddParameters(cmd, args);
			return cmd;
		}

		public void CreateTable(string destinationTable, IDataReader data) {
			throw new NotImplementedException();
		}

		public void Dispose() => connection.Dispose();

		public void Insert(string destinationTable, IDataReader rows, DataBossBulkCopySettings settings) {
			throw new NotImplementedException();
		}

		public void Open() => connection.Open();
	}
}
