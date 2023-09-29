using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;

namespace DataBoss.Data
{
	public interface ISqlDialect
	{
		string FormatName(string columnName);
		string GetTypeName(DataBossDbType dbType);
		bool TryCreateDialectSpecificParameter(string name, Expression readMember, out Expression create);

		IReadOnlyList<string> DataBossHistoryMigrations { get; }
		public string BeginMigrationQuery { get; }
		public string EndMigrationQuery { get; }
	}

	public interface IDataBossConnection : IDisposable
	{
		ISqlDialect Dialect { get; }
		ConnectionState State { get; }

		void Open();

		IDbTransaction BeginTransaction();
		void CreateTable(string destinationTable, IDataReader data);
		void Insert(string destinationTable, IDataReader rows, DataBossBulkCopySettings settings);
		Task InsertAsync(string destinationTable, DbDataReader rows, DataBossBulkCopySettings settings, CancellationToken cancellationToken = default);

		IDbCommand CreateCommand();
		IDbCommand CreateCommand(string cmdText);
		IDbCommand CreateCommand<T>(string cmdText, T args);
		IDbCommand CreateCommand(string cmdText, object args);

		void EnsureDatabase();
		int GetTableVersion(string tableName);
		void SetTableVersion(string tableName, int version);
		public string GetDefaultSchema();
	}
}