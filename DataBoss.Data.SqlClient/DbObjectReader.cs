#if MSSQLCLIENT
namespace DataBoss.Data.MsSql
{
	using Microsoft.Data.SqlClient;
#else
namespace DataBoss.Data
{
	using System.Data.SqlClient;
#endif

	using System;

	public static class DbObjectReader
	{
		public static DbObjectReader<SqlCommand, SqlDataReader> Create(SqlConnection connection) => new SqlObjectReader(connection.CreateCommand);
	}

	class SqlObjectReader : DbObjectReader<SqlCommand, SqlDataReader>
	{
		public SqlObjectReader(Func<SqlCommand> newCommand) : base(newCommand) { }
		
		public override void AddParameters<T>(SqlCommand cmd, T args) => ToParams.AddTo(cmd, args);
	}
}
