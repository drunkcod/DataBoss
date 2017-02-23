using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using DataBoss.Data;

namespace DataBoss
{
	public class DataBossBulkCopy
	{
		const string TempTableName = "#$";

		public readonly SqlConnection Connection;
		public readonly SqlTransaction Transaction;
		public int? CommandTimeout;

		public DataBossBulkCopy(SqlConnection connection) : this(connection, null) { }

		public DataBossBulkCopy(SqlConnection connection, SqlTransaction transaction) {
			this.Connection = connection;
			this.Transaction = transaction;
		}

		public void Insert(string destinationTable, IDataReader toInsert) {
			using (var bulkCopy = NewBulkCopy(destinationTable)) {
				for (var i = 0; i != toInsert.FieldCount; ++i) {
					var n = toInsert.GetName(i);
					bulkCopy.ColumnMappings.Add(n, n);
				}
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
			var bulkCopy = new SqlBulkCopy(Connection, SqlBulkCopyOptions.TableLock, Transaction) { DestinationTableName = destinationTable };
			if(CommandTimeout.HasValue)
				bulkCopy.BulkCopyTimeout = CommandTimeout.Value;
			return bulkCopy;
		}
	}
}