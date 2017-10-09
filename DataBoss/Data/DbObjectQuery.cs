using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;

namespace DataBoss.Data
{
	public struct DbObjectQuery
	{
		readonly Func<SqlCommand> getCommand;

		public DbObjectQuery(Func<SqlCommand> getCommand) {
			this.getCommand = getCommand;
		}

		public SqlCommandEnumerable<TOutput> Read<TOutput>(Func<SqlDataReader, Func<SqlDataReader, TOutput>> converterFactory) =>
			new SqlCommandEnumerable<TOutput>(getCommand, converterFactory);

		public SqlCommandEnumerable<TOutput> Read<TOutput>() => Read(x => ObjectReader.GetConverter<SqlDataReader, TOutput>(x, null));

		public List<TOutput> ReadWithRetry<TOutput>(RetryStrategy retry) => Read<TOutput>().ToList(retry);

		public TOutput Single<TOutput>() where TOutput : new() =>
			Read<TOutput>().Single();
	}
}
