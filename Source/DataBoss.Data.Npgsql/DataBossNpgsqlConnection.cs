using System;
using System.Data;
using System.Data.Common;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using DataBoss.Linq;
using Npgsql;
using NpgsqlTypes;

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
			var row = new object[rows.FieldCount];
			var dataType = new NpgsqlDbType?[rows.FieldCount];
			for (var i = 0; i != rows.FieldCount; ++i)
				dataType[i] = rows.GetDataTypeName(i) == "jsonb" ? NpgsqlDbType.Jsonb : null;
			var batchSize = settings.BatchSize ?? int.MaxValue;
			var copyFromCommand = CopyFromCommand(destinationTable, rows);
		begin:
			{
				var rowCount = 0;
				using var writer = connection.BeginBinaryImport(copyFromCommand);
				while (rows.Read()) {
					rows.GetValues(row);
					writer.StartRow();
					for (var i = 0; i != row.Length; ++i) {
						var dt = dataType[i];
						var v = row[i];
						if (dt.HasValue)
							writer.Write(v, dt.Value);
						else writer.Write(v);
					}
					if (++rowCount == batchSize) {
						writer.Complete();
						goto begin;
					}
				}
				writer.Complete();
			}
		}

		public async Task InsertAsync(string destinationTable, DbDataReader rows, DataBossBulkCopySettings settings, CancellationToken cancellationToken = default) {
			var row = new object[rows.FieldCount];
			var dataType = new NpgsqlDbType?[rows.FieldCount];
			for (var i = 0; i != rows.FieldCount; ++i)
				dataType[i] = rows.GetDataTypeName(i) == "jsonb" ? NpgsqlDbType.Jsonb : null;
			var batchSize = settings.BatchSize ?? int.MaxValue;
			var copyFromCommand = CopyFromCommand(destinationTable, rows);
		begin:
			{
				var rowCount = 0;
				using var writer = await connection.BeginBinaryImportAsync(copyFromCommand, cancellationToken).ConfigureAwait(false);
				while (await rows.ReadAsync(cancellationToken).ConfigureAwait(false)) {
					rows.GetValues(row);
					await writer.StartRowAsync(cancellationToken).ConfigureAwait(false);
					for (var i = 0; i != row.Length; ++i) {
						var dt = dataType[i];
						var v = row[i];
						await (dt.HasValue ? writer.WriteAsync(v, dt.Value, cancellationToken) : writer.WriteAsync(cancellationToken)).ConfigureAwait(false);
					}
					if (++rowCount == batchSize) {
						await writer.CompleteAsync(cancellationToken).ConfigureAwait(false);
						goto begin;
					}
				}
				await writer.CompleteAsync(cancellationToken).ConfigureAwait(false);
			}
		}

		static string CopyFromCommand(string destinationTable, IDataReader rows) => new StringBuilder()
			.Append("COPY ").Append(destinationTable).Append('(')
			.AppendJoin(',', Collection.ArrayInit(rows.FieldCount, x => $"\"{rows.GetName(x)}\""))
			.Append(") FROM STDIN(FORMAT BINARY)")
			.ToString();

		public void Open() => connection.Open();

		public void EnsureDatabase() {
			var cs = new NpgsqlConnectionStringBuilder(connection.ConnectionString) {
				Database = string.Empty,
			};
			using var c = new NpgsqlConnection(cs.ToString());
			c.Open();
			using var cmd = c.CreateCommand("select oid from pg_database where datname = :db", new { db = connection.Database });
			if (cmd.ExecuteScalar() is null) {
				cmd.ExecuteNonQuery($"create database \"{connection.Database}\"");
			}
		}

		public int GetTableVersion(string tableName) {
			using var c = CreateCommand(@"
				create table if not exists __DataBossMeta(
					tableName text unique not null,
					version int not null);
					
				select version, 0 from __DataBossMeta where tableName = :tableName", new { tableName });
			return (int)(c.ExecuteScalar() ?? 0);
		}

		public void SetTableVersion(string tableName, int version) {
			using var c = CreateCommand(
				"update __DataBossMeta set version = :version where tableName = :tableName returning version",
				new { tableName, version });
			if (c.ExecuteScalar() is null) {
				c.CommandText = "insert into __DataBossMeta values(:tableName, :version)";
				c.ExecuteNonQuery();
			}
		}

		public string GetDefaultSchema() => "public";
	}
}
