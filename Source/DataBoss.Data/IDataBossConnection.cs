using System;
using System.Collections.Generic;
using System.Data;
using System.Linq.Expressions;

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