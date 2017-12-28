using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using DataBoss.Data.Scripting;

namespace DataBoss.Data
{
	#pragma warning disable CS0649
	struct IdRow { public int Id; }
	#pragma warning restore CS0649

	public class DataBossBulkCopySettings
	{
		public int BatchSize;
		public int? CommandTimeout;
	}

	public class DataBossBulkCopy
	{
		const string TempTableName = "#$";

		readonly DataBossBulkCopySettings settings;

		public readonly SqlConnection Connection;
		public readonly SqlTransaction Transaction;
		
		int? CommandTimeout => settings.CommandTimeout;
		int BatchSize => settings.BatchSize; 

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

		public void Insert<T>(string destinationTable, IEnumerable<T> rows) => Insert(destinationTable, SequenceDataReader.Create(rows, x => x.MapAll()));

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
				output inserted.$identity as {nameof(IdRow.Id)}
				select {columns}
				from {TempTableName}
				order by [$]
			
				drop table {TempTableName}
			"))
			{
				if(CommandTimeout.HasValue)
					cmd.CommandTimeout = CommandTimeout.Value;
				cmd.Transaction = Transaction;
				using (var reader = ObjectReader.For(cmd.ExecuteReader(CommandBehavior.SingleResult | CommandBehavior.SequentialAccess))) { 
					var ids = reader.Read<IdRow>().Select(x => x.Id).ToList();
					ids.Sort();
					return ids;
				}
			}
		}

		SqlBulkCopy NewBulkCopy(string destinationTable) {
			var bulkCopy = new SqlBulkCopy(Connection, SqlBulkCopyOptions.TableLock, Transaction) { 
				DestinationTableName = destinationTable,
				EnableStreaming = true,
				BatchSize = BatchSize,
			};
			if(CommandTimeout.HasValue)
				bulkCopy.BulkCopyTimeout = CommandTimeout.Value;
			return bulkCopy;
		}
	}
}