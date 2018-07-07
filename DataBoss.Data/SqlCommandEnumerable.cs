using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;

namespace DataBoss.Data
{
	public class DbCommandEnumerable<TCommand, TReader, T> : IEnumerable<T>
		where TCommand : IDbCommand
		where TReader : IDataReader
	{
		static T NoRowsReturned() => throw new InvalidOperationException("No rows returned.");
		readonly Func<TCommand> getCommand;
		readonly Func<TCommand, TReader> executeReader;
		readonly Func<TReader, Func<TReader, T>> converterFactory;

		public DbCommandEnumerable(Func<TCommand> getCommand, Func<TCommand, TReader> executeReader, Func<TReader, Func<TReader, T>> converterFactory) {
			this.getCommand = getCommand;
			this.executeReader = executeReader;
			this.converterFactory = converterFactory;
		}

		public List<T> ToList(RetryStrategy retry) =>
			retry.Execute(() => new List<T>(this));

		public T Single(RetryStrategy retry) => SingleCore(retry, NoRowsReturned);
		public T SingleOrDefault(RetryStrategy retry) => SingleCore(retry, Lambdas.Default<T>);

		T SingleCore(RetryStrategy retry, Func<T> handleDefault) {
			for(var n = 1;; ++n) { 
				var it = GetEnumerator();
				try {
					if(!it.MoveNext())
						goto NoRow;
					var r = it.Current;
					if(it.MoveNext())
						goto TooManyRows;
					return r;
				} catch(Exception e) {
					if(!retry(n, e))
						throw;
				} finally {
					it.Dispose();
				}
			}
			NoRow: return handleDefault();
			TooManyRows: throw new InvalidOperationException("More than one result row.");
		}

		public IEnumerator<T> GetEnumerator() {
			var q = getCommand();
			if (string.IsNullOrEmpty(q.CommandText))
				return Enumerable.Empty<T>().GetEnumerator();
			var e = new DbReaderEnumerator(q, this);
			e.Reset();
			return e;
		}

		class DbReaderEnumerator : IEnumerator<T>
		{
			readonly DbCommandEnumerable<TCommand, TReader, T> parent;
			readonly TCommand command;
			Func<TReader, T> materialize;
			TReader reader;

			public DbReaderEnumerator(TCommand command, DbCommandEnumerable<TCommand, TReader, T> parent) {
				this.parent = parent;
				this.command = command;
			}

			public T Current => materialize(reader);
			object IEnumerator.Current => Current;

			public void Dispose() {
				if(reader != null) {
					command.Cancel();
					reader.Dispose();
				}
				command.Dispose();
			}

			public bool MoveNext() {
				if(reader == null)
					return false;
				read: if(!reader.Read()) {
					if(reader.NextResult())
						goto read;
					reader.Dispose();
					reader = default(TReader);
					return false;
				}
				return true;
			}

			public void Reset() { 
				reader = parent.executeReader(command);
				try { 
					materialize = parent.converterFactory(reader);
				} catch {
					reader.Dispose();
					throw;
				}
			}
		}

		IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
	}
}
