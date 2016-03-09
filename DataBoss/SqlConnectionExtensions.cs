using System.Data.SqlClient;

namespace DataBoss
{
	public static class SqlConnectionExtensions
	{
		public static SqlCommand CreateCommand(this SqlConnection connection, string cmdText) {
			return new SqlCommand(cmdText, connection);
		}

		public static SqlCommand CreateCommand<T>(this SqlConnection connection, string cmdText, T args) {
			var cmd = new SqlCommand(cmdText, connection);
			cmd.Parameters.AddRange(ToParams.Invoke(args));
			return cmd;
		}
	}
}