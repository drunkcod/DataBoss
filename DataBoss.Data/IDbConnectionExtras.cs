using System.Data;

namespace DataBoss.Data
{
	public interface IDataBossConnectionExtras
	{
		void CreateTable(string destinationTable, IDataReader data);
		void Insert(string destinationTable, IDataReader rows, DataBossBulkCopySettings settings);
	}
}