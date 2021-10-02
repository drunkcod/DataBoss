namespace DataBoss.Data
{
	using System;
	using System.Collections.Generic;
	using System.Data;
	using System.Linq;

	public static class DbObjectQuery
	{
		public static DbObjectQuery<IDbCommand, IDataReader> Create(Func<IDbCommand> getCommand) =>
			new(getCommand, x => x.ExecuteReader());

		public static DbObjectQuery<IDbCommand, IDataReader> Create(Func<IDbCommand> getCommand, bool bufferResult) =>
			new(getCommand, x => x.ExecuteReader(), bufferResult: bufferResult);

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

	public readonly struct DbObjectQuery<TCommand, TReader> 
		where TCommand : IDbCommand 
		where TReader: IDataReader
	{
		readonly Func<TCommand> getCommand;
		readonly Func<TCommand, TReader> executeReader;
		readonly bool bufferResult;

		public DbObjectQuery(Func<TCommand> getCommand, Func<TCommand, TReader> executeReader, bool bufferResult = false) {
			this.getCommand = getCommand;
			this.executeReader = executeReader;
			this.bufferResult = bufferResult;
		}

		public IEnumerable<TResult> Read<TResult>() => 
			BufferOrNot(MakeEnumerable(DefaultReader<TResult>, null));

		public IEnumerable<TOutput> Read<TFun, TOutput>(TFun fun) where TFun : Delegate => 
			BufferOrNot(MakeEnumerable<TFun, TOutput>(fun));

		IEnumerable<T> BufferOrNot<T>(IEnumerable<T> xs) => bufferResult ? xs.ToList() : xs;

		static Func<TReader, T> DefaultReader<T>(TReader reader, object _) =>
			ConverterFactory.Default.GetConverter<TReader, T>(reader).Compiled;

		public DbCommandEnumerable<TCommand, TReader, TOutput> Read<TOutput>(ConverterCollection converters) => MakeEnumerable(GetConverter<TOutput>, converters);
		
		static Func<TReader, TOutput> GetConverter<TOutput>(TReader reader, object state) =>
			ObjectReader.GetConverter<TReader, TOutput>(reader, (ConverterCollection)state);

		public DbCommandEnumerable<TCommand, TReader, TOutput> MakeEnumerable<TOutput>(Func<TReader, object, Func<TReader, TOutput>> converterFactory, object state) =>
			new(getCommand, executeReader, converterFactory, state);

		public DbCommandEnumerable<TCommand, TReader, TOutput> MakeEnumerable<TFun, TOutput>(TFun fun) where TFun : Delegate => 
			new(getCommand, executeReader, DbObjectQuery.NewFuncConverter<TReader, TFun,TOutput>, fun);
		
		public List<TOutput> ReadWithRetry<TOutput>(RetryStrategy retry) => MakeEnumerable(DefaultReader<TOutput>, null).ToList(retry);
		public List<TOutput> ReadWithRetry<TOutput>(RetryStrategy retry, ConverterCollection converters) => MakeEnumerable(DefaultReader<TOutput>, converters).ToList(retry);

		public TOutput Single<TOutput>() where TOutput : new() =>
			MakeEnumerable(DefaultReader<TOutput>, null).Single();
	}
}
