using System.Data;

namespace DataBoss.Data
{
	interface IDataBossConnectionExtras
	{
		void CreateTable(string destinationTable, IDataReader data);
		void Insert(string destinationTable, IDataReader rows, DataBossBulkCopySettings settings);
	}
}