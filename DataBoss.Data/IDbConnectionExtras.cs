using System.Data;

namespace DataBoss.Data
{
	interface IDbConnectionExtras
	{
		void CreateTable(string destinationTable, IDataReader data);
		void Insert(string destinationTable, IDataReader rows, DataBossBulkCopySettings settings);
	}
}