using System.Data;

namespace DataBoss.Data
{
	public interface IDataBossConnection
	{
		string ParameterPrefix { get; }

		void CreateTable(string destinationTable, IDataReader data);
		void Insert(string destinationTable, IDataReader rows, DataBossBulkCopySettings settings);
		IDbCommand CreateCommand(string cmdText);
		IDbCommand CreateCommand<T>(string cmdText, T args);
	}
}