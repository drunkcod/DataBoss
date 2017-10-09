using System;
using System.Collections;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Threading;
using System.Linq;

namespace DataBoss.Data
{
	public delegate TimeSpan? RetryStrategy(int retryAttempt, Exception problem);

	public class SqlCommandEnumerable<T> : IEnumerable<T>
	{
		readonly Func<SqlCommand> getCommand;
		readonly Func<SqlDataReader, Func<SqlDataReader, T>> converterFactory;

		public SqlCommandEnumerable(Func<SqlCommand> getCommand, Func<SqlDataReader, Func<SqlDataReader, T>> converterFactory) {
			this.getCommand = getCommand;
			this.converterFactory = converterFactory;
		}

		public List<T> ToList(RetryStrategy retry) {
			for(var n = 1;; ++n) { 
				try {
					return new List<T>(this);
				} catch(Exception e) {
					var again = retry(n, e);
					if(!again.HasValue)
						throw;
					Thread.Sleep(again.Value);
				}
			}
		}

		public T Single(RetryStrategy retry) {
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
					var again = retry(n, e);
					if(!again.HasValue)
						throw;
					Thread.Sleep(again.Value);
				} finally {
					it.Dispose();
				}
			}
			NoRow: throw new InvalidOperationException("No rows returned.");
			TooManyRows: throw new InvalidOperationException("More than one result row.");
		}

		public T SingleOrDefault(RetryStrategy retry) {
			for(var n = 1;; ++n) { 
				var it = GetEnumerator();
				try {
					if(!it.MoveNext())
						return default(T);
					var r = it.Current;
					if(it.MoveNext())
						goto TooManyRows;
					return r;
				} catch(Exception e) {
					var again = retry(n, e);
					if(!again.HasValue)
						throw;
					Thread.Sleep(again.Value);
				} finally {
					it.Dispose();
				}
			}
			TooManyRows: throw new InvalidOperationException("More than one result row.");
		}

		public IEnumerator<T> GetEnumerator() {
			using(var q = getCommand()) {
				if(string.IsNullOrEmpty(q.CommandText))
					yield break;
				using(var r = q.ExecuteReader()) {
					var materialize = converterFactory(r);
					do {
						while(r.Read())
							yield return materialize(r);
					} while(r.NextResult());
				}
			}
		}

		IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
	}
}
