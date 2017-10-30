using DataBoss.Data;
using System.Data.SqlClient;

namespace DataBoss
{
	public static class SqlCommandExtensions
	{
		public static SqlDataReader ExecuteReader(this SqlCommand cmd, string cmdText) {
			cmd.CommandText = cmdText;
			return cmd.ExecuteReader();
		}

		public static SqlDataReader ExecuteReader<T>(this SqlCommand cmd, string cmdText, T args) {
			cmd.CommandText = cmdText;
			cmd.Parameters.Clear();
			cmd.Parameters.AddRange(ToParams.Invoke(args));
			return cmd.ExecuteReader();
		}

		public static object ExecuteScalar(this SqlCommand cmd, string cmdText) {
			cmd.CommandText = cmdText;
			return cmd.ExecuteScalar();
		}

		public static object ExecuteScalar<T>(this SqlCommand cmd, string cmdText, T args) {
			cmd.CommandText = cmdText;
			cmd.Parameters.Clear();
			cmd.Parameters.AddRange(ToParams.Invoke(args));
			return cmd.ExecuteScalar();
		}

		public static int ExecuteNonQuery(this SqlCommand cmd, string cmdText) {
			cmd.CommandText = cmdText;
			return cmd.ExecuteNonQuery();
		}

		public static int ExecuteNonQuery<T>(this SqlCommand cmd, string cmdText, T args) {
			cmd.CommandText = cmdText;
			cmd.Parameters.Clear();
			cmd.Parameters.AddRange(ToParams.Invoke(args));
			return cmd.ExecuteNonQuery();
		}
	}
}