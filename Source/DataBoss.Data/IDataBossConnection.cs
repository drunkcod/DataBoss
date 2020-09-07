using System.Data;
using System.Linq.Expressions;

namespace DataBoss.Data
{
	public interface ISqlDialect
	{
		string ParameterPrefix { get; }

		Expression MakeRowVersionParameter(string name, Expression readMember);
	}

	public interface IDataBossConnection
	{
		ISqlDialect Dialect { get; }

		void CreateTable(string destinationTable, IDataReader data);
		void Insert(string destinationTable, IDataReader rows, DataBossBulkCopySettings settings);
		IDbCommand CreateCommand(string cmdText);
		IDbCommand CreateCommand<T>(string cmdText, T args);
	}
}