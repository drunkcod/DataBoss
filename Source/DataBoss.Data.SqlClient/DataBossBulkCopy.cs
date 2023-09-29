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
	using System.Linq;
	using DataBoss.Data.Common;
	using DataBoss.Data.Scripting;
	using System.Data.Common;
	using System.Threading.Tasks;
	using System.Threading;

	public class DataBossBulkCopy
	{
		const string TempTableName = "#$";

		class SqlBulkCopyContext
		{
			readonly SqlConnection connection;
			readonly SqlTransaction transaction;
			readonly DataBossBulkCopySettings settings;

			public SqlBulkCopyContext(SqlConnection connection, SqlTransaction transaction, DataBossBulkCopySettings settings) {
				this.connection = connection;
				this.transaction = transaction;
				this.settings = settings;
			}

			public IDbCommand CreateCommand() {
				var cmd = connection.CreateCommand();
				cmd.Transaction = transaction;
				if (settings.CommandTimeout.HasValue)
					cmd.CommandTimeout = settings.CommandTimeout.Value;
				return cmd;
			}

			SqlBulkCopy CreateBulkCopy(string destinationTable, Action<SqlBulkCopyColumnMappingCollection> map) {
				var bulkCopy = new SqlBulkCopy(connection, (SqlBulkCopyOptions)(settings.Options ?? DataBossBulkCopyOptions.TableLock), transaction) {
					DestinationTableName = destinationTable,
					EnableStreaming = true,
				};
				if (settings.BatchSize.HasValue)
					bulkCopy.BatchSize = settings.BatchSize.Value;
				if (settings.CommandTimeout.HasValue)
					bulkCopy.BulkCopyTimeout = settings.CommandTimeout.Value;

				map(bulkCopy.ColumnMappings);
				return bulkCopy;

			}

			public void WriteToServer(IDataReader rows, string destinationTable, Action<SqlBulkCopyColumnMappingCollection> map) {
				using var bulkCopy = CreateBulkCopy(destinationTable, map);
				bulkCopy.WriteToServer(rows);
			}

			public async Task WriteToServerAsync(DbDataReader rows, string destinationTable, Action<SqlBulkCopyColumnMappingCollection> map, CancellationToken cancellationToken = default) {
				using var bulkCopy = CreateBulkCopy(destinationTable, map);
				await bulkCopy.WriteToServerAsync(rows, cancellationToken);
			}
		}

		readonly SqlBulkCopyContext context;

		public DataBossBulkCopy(SqlConnection connection) : this(connection, null, new DataBossBulkCopySettings()) { }
		public DataBossBulkCopy(SqlConnection connection, DataBossBulkCopySettings settings) : this(connection, null, settings) { }
		public DataBossBulkCopy(SqlConnection connection, SqlTransaction transaction) : this(connection, transaction, new DataBossBulkCopySettings()) { }

		public DataBossBulkCopy(SqlConnection connection, SqlTransaction transaction, DataBossBulkCopySettings settings) {
			this.context = new SqlBulkCopyContext(connection, transaction, settings);
		}

		public void Insert(string destinationTable, IDataReader toInsert) {
			context.WriteToServer(toInsert, $"[{destinationTable}]", columns => {
				for (int i = 0, fieldCount = toInsert.FieldCount; i != fieldCount; ++i)
					columns.Add(i, toInsert.GetName(i));
			});
		}

		public Task InsertAsync(string destinationTable, DbDataReader toInsert, CancellationToken cancellationToken = default) {
			return context.WriteToServerAsync(toInsert, $"[{destinationTable}]", columns => {
				for (int i = 0, fieldCount = toInsert.FieldCount; i != fieldCount; ++i)
					columns.Add(i, toInsert.GetName(i));
			}, cancellationToken);
		}

		public void Insert<T>(string destinationTable, IEnumerable<T> rows) => 
			Insert(destinationTable, SequenceDataReader.Create(rows, x => x.MapAll()));

		public ICollection<int> InsertAndGetIdentities<T>(string destinationTable, IEnumerable<T> rows) {
			var n = 0;
			var toInsert = SequenceDataReader.Create(rows, x => {
				x.Map("$", _ => ++n);
				x.MapAll();
			});

			using(var cmd = context.CreateCommand()) {
				cmd.CommandText =$@"
					{MsSqlDialect.Scripter.ScriptTable(TempTableName, toInsert)}
					create clustered index [#$_$] on {TempTableName}([{toInsert.GetName(0)}])";
				cmd.ExecuteNonQuery();

				context.WriteToServer(toInsert, TempTableName, _ => { });

				var columns = string.Join(",", Enumerable.Range(1, toInsert.FieldCount - 1).Select(toInsert.GetName));

				cmd.CommandText = $@"
					insert {destinationTable} with(tablock)({columns})
					output inserted.$identity as {nameof(IdRow<int>.Id)}
					select {columns}
					from {TempTableName}
					order by [$]
			
					drop table {TempTableName}";
				var ids = new List<int>(n);
				var reader = ObjectReader.For(() => cmd.ExecuteReader(CommandBehavior.SingleResult | CommandBehavior.SequentialAccess)); 
				reader.Read<IdRow<int>>(x => ids.Add(x.Id));
				ids.Sort();
				return ids;
			}
		}
	}
}