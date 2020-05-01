using System.Data;

namespace DataBoss.Data
{
	public interface IDataBossConnection
	{
		void CreateTable(string destinationTable, IDataReader data);
		void Insert(string destinationTable, IDataReader rows, DataBossBulkCopySettings settings);
		IDbCommand CreateCommand<T>(string cmdText, T args);
	}
}