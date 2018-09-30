using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using DataBoss.Data.Common;
using DataBoss.Data.Scripting;

namespace DataBoss.Data
{
	public struct DataBossBulkCopySettings
	{
		public int? BatchSize;
		public int? CommandTimeout;
		public SqlBulkCopyOptions? Options;

		public DataBossBulkCopySettings WithCommandTimeout(int? value) {
			var x = this;
			x.CommandTimeout = value;
			return x;
		}
	}

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

			public void WriteToServer(IDataReader rows, string destinationTable, Action<SqlBulkCopyColumnMappingCollection> map) {
				using(var bulkCopy = new SqlBulkCopy(connection, settings.Options ?? SqlBulkCopyOptions.TableLock, transaction) {
					DestinationTableName = destinationTable,
					EnableStreaming = true,
				}) { 
					if (settings.BatchSize.HasValue)
						bulkCopy.BatchSize = settings.BatchSize.Value;
					if (settings.CommandTimeout.HasValue)
						bulkCopy.BulkCopyTimeout = settings.CommandTimeout.Value;

					map(bulkCopy.ColumnMappings);
					bulkCopy.WriteToServer(rows);
				}
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
			context.WriteToServer(toInsert, destinationTable, columns => {
				for (int i = 0, fieldCount = toInsert.FieldCount; i != fieldCount; ++i)
					columns.Add(i, toInsert.GetName(i));
			});
		}

		public void Insert<T>(string destinationTable, IEnumerable<T> rows) => 
			Insert(destinationTable, SequenceDataReader.Create(rows, x => x.MapAll()));

		public ICollection<int> InsertAndGetIdentities<T>(string destinationTable, IEnumerable<T> rows) {
			var n = 0;
			var toInsert = SequenceDataReader.Create(rows, x => {
				x.Map("$", _ => ++n);
				x.MapAll();
			});

			var scripter = new DataBossScripter();
			using(var cmd = context.CreateCommand()) {
				cmd.CommandText =$@"
					{scripter.ScriptTable(TempTableName, toInsert)}
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
				using (var reader = ObjectReader.For(cmd.ExecuteReader(CommandBehavior.SingleResult | CommandBehavior.SequentialAccess))) { 
					reader.Read<IdRow<int>>(x => ids.Add(x.Id));
					ids.Sort();
					return ids;
				}
			}
		}
	}
}