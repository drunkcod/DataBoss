using System;
using System.Data;

namespace DataBoss.Data
{
	public static class DbOps<T, TReader>
	{
		public static readonly Func<T, object> ExecuteScalar = Lambdas.Func<T, object>(nameof(IDbCommand.ExecuteScalar));
		public static readonly Func<T, int> ExecuteNonQuery = Lambdas.Func<T, int>(nameof(IDbCommand.ExecuteNonQuery));
		public static readonly Func<T, TReader> ExecuteReader = Lambdas.Func<T, TReader>(nameof(IDbCommand.ExecuteReader));
	}


}
