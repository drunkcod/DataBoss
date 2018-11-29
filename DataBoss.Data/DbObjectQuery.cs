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


		internal static Func<TReader, TOutput> NewFuncConverter<TReader, TFun, TOutput> (TReader reader, object state) 
			where TReader : IDataReader
			where TFun : Delegate 
			=> new FuncConverter<TReader, TFun, TOutput>(reader, (TFun)state).Materialize;

		internal class FuncConverter<TReader, TFun, TOutput> 
			where TReader : IDataReader
			where TFun : Delegate
		{
			readonly Func<TReader, TFun, TOutput> trampoline;
			readonly TFun fun;

			public FuncConverter(TReader reader, TFun fun) {
				this.trampoline = (Func<TReader, TFun, TOutput>)ConverterFactory.Default.CompileTrampoline(reader, fun);
				this.fun = fun;
			}

			public TOutput Materialize(TReader reader) => trampoline(reader, fun);
		}
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

		public DbCommandEnumerable<TCommand, TReader, TResult> Read<TResult>() => Read(DefaultReader<TResult>, null);

		public DbCommandEnumerable<TCommand, TReader, TResult> Read<T1, TResult>(Func<T1, TResult> factory) =>
			Read<Func<T1, TResult>, TResult>(factory);
		
		public DbCommandEnumerable<TCommand, TReader, TResult> Read<T1, T2, TResult>(Func<T1, T2, TResult> factory) =>
			Read<Func<T1, T2, TResult>, TResult>(factory);

		public DbCommandEnumerable<TCommand, TReader, TResult> Read<T1, T2, T3, TResult>(Func<T1, T2, T3, TResult> factory) =>
			Read<Func<T1, T2, T3, TResult>, TResult>(factory);

		public DbCommandEnumerable<TCommand, TReader, TResult> Read<T1, T2, T3, T4, TResult>(Func<T1, T2, T3, T4, TResult> factory) =>
			Read<Func<T1, T2, T3, T4, TResult>, TResult>(factory);

		public DbCommandEnumerable<TCommand, TReader, TResult> Read<T1, T2, T3, T4, T5, TResult>(Func<T1, T2, T3, T4, T5, TResult> factory) =>
			Read<Func<T1, T2, T3, T4, T5, TResult>, TResult>(factory);

		public DbCommandEnumerable<TCommand, TReader, TResult> Read<T1, T2, T3, T4, T5, T6, TResult>(Func<T1, T2, T3, T4, T5, T6, TResult> factory) =>
			Read<Func<T1, T2, T3, T4, T5, T6, TResult>, TResult>(factory);

		public DbCommandEnumerable<TCommand, TReader, TResult> Read<T1, T2, T3, T4, T5, T6, T7, TResult>(Func<T1, T2, T3, T4, T5, T6, T7, TResult> factory) =>
			Read<Func<T1, T2, T3, T4, T5, T6, T7, TResult>, TResult>(factory);

		static Func<TReader, T> DefaultReader<T>(TReader reader, object _) =>
			ConverterFactory.Default.GetConverter<TReader, T>(reader).Compiled;

		public DbCommandEnumerable<TCommand, TReader, TOutput> Read<TOutput>(ConverterCollection converters) => Read(GetConverter<TOutput>, converters);
		static Func<TReader, TOutput> GetConverter<TOutput>(TReader reader, object state) => ObjectReader.GetConverter<TReader, TOutput>(reader, (ConverterCollection)state);

		public DbCommandEnumerable<TCommand, TReader, TOutput> Read<TOutput>(Func<TReader, object, Func<TReader, TOutput>> converterFactory, object state) =>
			new DbCommandEnumerable<TCommand, TReader, TOutput>(getCommand, executeReader, converterFactory, state);

		public DbCommandEnumerable<TCommand, TReader, TOutput> Read<TFun, TOutput>(TFun fun) where TFun : Delegate { 
			return new DbCommandEnumerable<TCommand, TReader, TOutput>(getCommand, executeReader, DbObjectQuery.NewFuncConverter<TReader, TFun,TOutput>, fun);
		}

		public List<TOutput> ReadWithRetry<TOutput>(RetryStrategy retry) => Read<TOutput>().ToList(retry);
		public List<TOutput> ReadWithRetry<TOutput>(RetryStrategy retry, ConverterCollection converters) => Read<TOutput>(converters).ToList(retry);

		public TOutput Single<TOutput>() where TOutput : new() =>
			Read<TOutput>().Single();
	}
}
