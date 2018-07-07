using System;
using System.Data;
using System.Data.SqlClient;

namespace DataBoss.Data
{
	static class Ops
	{
		public static readonly Action<SqlConnection> Dispose = Lambdas.Action<SqlConnection>(nameof(SqlConnection.Dispose));
	}

	public static class DbOps<T, TReader>
	{
		public static readonly Func<T, object> ExecuteScalar = Lambdas.Func<T, object>(nameof(IDbCommand.ExecuteScalar));
		public static readonly Func<T, int> ExecuteQuery = Lambdas.Func<T, int>(nameof(IDbCommand.ExecuteNonQuery));
		public static readonly Func<T, TReader> ExecuteReader = Lambdas.Func<T, TReader>(nameof(IDbCommand.ExecuteReader));
	}

}
