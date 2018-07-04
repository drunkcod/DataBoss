using System;
using System.Collections;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;

namespace DataBoss.Data
{
	public class SqlCommandEnumerable<T> : IEnumerable<T>
	{
		static T NoRowsReturned() => throw new InvalidOperationException("No rows returned.");
		readonly Func<SqlCommand> getCommand;
		readonly Func<SqlDataReader, Func<SqlDataReader, T>> converterFactory;

		public SqlCommandEnumerable(Func<SqlCommand> getCommand, Func<SqlDataReader, Func<SqlDataReader, T>> converterFactory) {
			this.getCommand = getCommand;
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
			var r = q.ExecuteReader();
			try { 
				return new SqlReaderEnumerator(q, r, converterFactory(r));
			}
			catch {
				r.Dispose();
				throw;
			}
		}

		class SqlReaderEnumerator : IEnumerator<T>
		{
			readonly SqlCommand command;
			readonly Func<SqlDataReader, T> materialize;
			SqlDataReader reader;

			public SqlReaderEnumerator(SqlCommand command, SqlDataReader reader, Func<SqlDataReader, T> materialize) {
				this.command = command;
				this.reader = reader;
				this.materialize = materialize;
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
					reader = null;
					return false;
				}
				return true;
			}

			public void Reset() => reader = command.ExecuteReader();
		}

		IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
	}
}
