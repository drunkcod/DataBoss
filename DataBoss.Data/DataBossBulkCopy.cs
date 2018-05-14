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
	}

	public class DataBossBulkCopy
	{
		const string TempTableName = "#$";

		readonly DataBossBulkCopySettings settings;

		public readonly SqlConnection Connection;
		public readonly SqlTransaction Transaction;
		
		public DataBossBulkCopy(SqlConnection connection) : this(connection, null, new DataBossBulkCopySettings()) { }
		public DataBossBulkCopy(SqlConnection connection, DataBossBulkCopySettings settings) : this(connection, null, settings) { }
		public DataBossBulkCopy(SqlConnection connection, SqlTransaction transaction) : this(connection, transaction, new DataBossBulkCopySettings()) { }

		public DataBossBulkCopy(SqlConnection connection, SqlTransaction transaction, DataBossBulkCopySettings settings) {
			this.Connection = connection;
			this.Transaction = transaction;
			this.settings = settings;
		}

		public void Insert(string destinationTable, IDataReader toInsert) {
			using (var bulkCopy = NewBulkCopy(destinationTable)) {
				for (int i = 0, fieldCount = toInsert.FieldCount; i != fieldCount; ++i)
					bulkCopy.ColumnMappings.Add(i, toInsert.GetName(i));
				bulkCopy.WriteToServer(toInsert);
			}
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
			Connection.ExecuteNonQuery(Transaction, $@"
				{scripter.ScriptTable(TempTableName, toInsert)}
				create clustered index [#$_$] on {TempTableName}([{toInsert.GetName(0)}])
			");

			using (var bulkCopy = NewBulkCopy(TempTableName))
				bulkCopy.WriteToServer(toInsert);

			var columns = string.Join(",", Enumerable.Range(1, toInsert.FieldCount - 1).Select(toInsert.GetName));
			using (var cmd = Connection.CreateCommand($@"
				insert {destinationTable} with(tablock)({columns})
				output inserted.$identity as {nameof(IdRow<int>.Id)}
				select {columns}
				from {TempTableName}
				order by [$]
			
				drop table {TempTableName}
			"))
			{
				if(settings.CommandTimeout.HasValue)
					cmd.CommandTimeout = settings.CommandTimeout.Value;
				cmd.Transaction = Transaction;
				var ids = new List<int>(n);
				using (var reader = ObjectReader.For(cmd.ExecuteReader(CommandBehavior.SingleResult | CommandBehavior.SequentialAccess))) { 
					reader.Read<IdRow<int>>(x => ids.Add(x.Id));
					ids.Sort();
					return ids;
				}
			}
		}

		SqlBulkCopy NewBulkCopy(string destinationTable) {
			var bulkCopy = new SqlBulkCopy(Connection, SqlBulkCopyOptions.TableLock, Transaction) { 
				DestinationTableName = destinationTable,
				EnableStreaming = true,
			};
			if(settings.BatchSize.HasValue)
				bulkCopy.BatchSize = settings.BatchSize.Value;
			if(settings.CommandTimeout.HasValue)
				bulkCopy.BulkCopyTimeout = settings.CommandTimeout.Value;
			return bulkCopy;
		}
	}
}