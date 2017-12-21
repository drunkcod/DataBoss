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

		public SqlCommandEnumerable<TOutput> Read<TOutput>() => Read(x => ObjectReader.GetConverter<SqlDataReader, TOutput>(x, null));

		public SqlCommandEnumerable<TOutput> Read<TOutput>(ConverterCollection converters) => 
			Read(x => ObjectReader.GetConverter<SqlDataReader, TOutput>(x, converters));

		public SqlCommandEnumerable<TOutput> Read<TOutput>(Func<SqlDataReader, Func<SqlDataReader, TOutput>> converterFactory) =>
			new SqlCommandEnumerable<TOutput>(getCommand, converterFactory);

		public List<TOutput> ReadWithRetry<TOutput>(RetryStrategy retry) => Read<TOutput>().ToList(retry);
		public List<TOutput> ReadWithRetry<TOutput>(RetryStrategy retry, ConverterCollection converters) => Read<TOutput>(converters).ToList(retry);

		public TOutput Single<TOutput>() where TOutput : new() =>
			Read<TOutput>().Single();
	}
}
