using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;

namespace DataBoss.Data
{
	public static class DbObjectQuery
	{
		public static DbObjectQuery<IDbCommand, IDataReader> Create(Func<IDbCommand> getCommand) =>
			new DbObjectQuery<IDbCommand, IDataReader>(getCommand, DbOps<IDbCommand, IDataReader>.ExecuteReader);
	}

	public struct DbObjectQuery<TCommand, TReader> 
		where TCommand : IDbCommand 
		where TReader: IDataReader
	{
		readonly Func<TCommand> getCommand;
		readonly Func<TCommand, TReader> executeReader;

		public DbObjectQuery(Func<TCommand> getCommand, Func<TCommand, TReader> executeReader) {
			this.getCommand = getCommand;
			this.executeReader = executeReader;
		}

		public DbCommandEnumerable<TCommand, TReader, TOutput> Read<TOutput>() =>
			Read(ObjectReader.GetConverter<TReader, TOutput>);

		public DbCommandEnumerable<TCommand, TReader, TOutput> Read<TOutput>(ConverterCollection converters) => 
			Read(converters.GetConverter<TReader, TOutput>);

		public DbCommandEnumerable<TCommand, TReader, TOutput> Read<TOutput>(Func<TReader, Func<TReader, TOutput>> converterFactory) =>
			new DbCommandEnumerable<TCommand, TReader, TOutput>(getCommand, executeReader, converterFactory);

		public List<TOutput> ReadWithRetry<TOutput>(RetryStrategy retry) => Read<TOutput>().ToList(retry);
		public List<TOutput> ReadWithRetry<TOutput>(RetryStrategy retry, ConverterCollection converters) => Read<TOutput>(converters).ToList(retry);

		public TOutput Single<TOutput>() where TOutput : new() =>
			Read<TOutput>().Single();
	}
}
