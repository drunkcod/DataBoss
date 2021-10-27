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

	public static class SqlCommandExtensions
	{
		static internal readonly EventHandler DisposeConnection = (sender, _) => ((SqlCommand)sender).Connection.Dispose();

		public static void AddParameters<T>(this SqlCommand command, T args) =>
			MsSqlDialect.AddParameters(command, args);

		public static void AddParameters(this SqlCommand command, object args) =>
			MsSqlDialect.AddParameters(command, args);

		public static SqlDataReader ExecuteReader(this SqlCommand cmd, RetryStrategy retry) =>
			retry.Execute(() => cmd.ExecuteReader());

		public static SqlDataReader ExecuteReader(this SqlCommand cmd, string cmdText) {
			cmd.CommandText = cmdText;
			return cmd.ExecuteReader();
		}

		public static SqlDataReader ExecuteReader(this SqlCommand cmd, string cmdText, RetryStrategy retry) =>
			retry.Execute(() => cmd.ExecuteReader(cmdText));

		public static SqlDataReader ExecuteReader(this SqlCommand cmd, string cmdText, object args) =>
			cmd.WithQuery(cmdText, args).ExecuteReader();

		public static SqlDataReader ExecuteReader(this SqlCommand cmd, string cmdText, object args, RetryStrategy retry) =>
			retry.Execute(() => cmd.ExecuteReader(cmdText, args));

		public static SqlDataReader ExecuteReader<T>(this SqlCommand cmd, string cmdText, T args) =>
			cmd.WithQuery(cmdText, args).ExecuteReader();

		public static SqlDataReader ExecuteReader<T>(this SqlCommand cmd, string cmdText, T args, RetryStrategy retry) =>
			retry.Execute(() => cmd.ExecuteReader(cmdText, args));

		public static object ExecuteScalar(this SqlCommand cmd, string cmdText) {
			cmd.CommandText = cmdText;
			return cmd.ExecuteScalar();
		}

		public static object ExecuteScalar(this SqlCommand cmd, string cmdText, RetryStrategy retry) =>
			retry.Execute(() => cmd.ExecuteScalar(cmdText));

		public static object ExecuteScalar(this SqlCommand cmd, string cmdText, object args) =>
			cmd.WithQuery(cmdText, args).ExecuteScalar();

		public static object ExecuteScalar(this SqlCommand cmd, string cmdText, object args, RetryStrategy retry) =>
			retry.Execute(() => cmd.ExecuteScalar(cmdText, args));

		public static object ExecuteScalar<T>(this SqlCommand cmd, string cmdText, T args) =>
			cmd.WithQuery(cmdText, args).ExecuteScalar();

		public static object ExecuteScalar<T>(this SqlCommand cmd, string cmdText, T args, RetryStrategy retry) =>
			retry.Execute(() => cmd.ExecuteScalar(cmdText, args));

		public static int ExecuteNonQuery(this SqlCommand cmd, string cmdText) {
			cmd.CommandText = cmdText;
			return cmd.ExecuteNonQuery();
		}

		public static int ExecuteNonQuery(this SqlCommand cmd, string cmdText, RetryStrategy retry) =>
			retry.Execute(() => cmd.ExecuteNonQuery(cmdText));

		public static int ExecuteNonQuery(this SqlCommand cmd, string cmdText, object args) =>
			cmd.WithQuery(cmdText, args).ExecuteNonQuery();

		public static int ExecuteNonQuery(this SqlCommand cmd, string cmdText, object args, RetryStrategy retry) =>
			retry.Execute(() => cmd.ExecuteNonQuery(cmdText, args));

		public static int ExecuteNonQuery<T>(this SqlCommand cmd, string cmdText, T args) =>
			cmd.WithQuery(cmdText, args).ExecuteNonQuery();

		public static int ExecuteNonQuery<T>(this SqlCommand cmd, string cmdText, T args, RetryStrategy retry) =>
			retry.Execute(() => cmd.ExecuteNonQuery(cmdText, args));

		public static SqlCommand Open(string connectionString) {
			var cmd = new SqlCommand {
				Connection = new SqlConnection(connectionString),
			};
			cmd.Disposed += DisposeConnection;
			cmd.Connection.Open();
			return cmd;
		}

		static SqlCommand WithQuery(this SqlCommand cmd, string cmdText, object args) {
			cmd.CommandText = cmdText;
			cmd.Parameters.Clear();
			cmd.AddParameters(args);
			return cmd;
		}

		static SqlCommand WithQuery<T>(this SqlCommand cmd, string cmdText, T args) {
			cmd.CommandText = cmdText;
			cmd.Parameters.Clear();
			cmd.AddParameters(args);
			return cmd;
		}
	}
}