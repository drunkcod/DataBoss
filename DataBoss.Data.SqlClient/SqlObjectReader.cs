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

	public class SqlObjectReader : DbObjectReader<SqlCommand, SqlDataReader>
	{
		public static SqlObjectReader Create(SqlConnection connection) => new SqlObjectReader(connection.CreateCommand);

		public SqlObjectReader(Func<SqlCommand> newCommand) : base(newCommand) { }
		
		public override void AddParameters<T>(SqlCommand cmd, T args) => ToParams.AddTo(cmd, args);
	}
}
