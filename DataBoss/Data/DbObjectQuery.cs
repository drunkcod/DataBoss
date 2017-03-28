using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Threading;

namespace DataBoss.Data
{
	public delegate TimeSpan? RetryStrategy(int retryAttempt, Exception problem);

	public struct DbObjectQuery
	{
		readonly Func<SqlCommand> getCommand;

		public DbObjectQuery(Func<SqlCommand> getCommand) {
			this.getCommand = getCommand;
		}

		public IEnumerable<TOutput> Read<TOutput>() {
			using(var q = getCommand()) {
				if(string.IsNullOrEmpty(q.CommandText))
					yield break;
				using(var r = ObjectReader.For(q.ExecuteReader()))
					do {
						foreach(var item in r.Read<TOutput>())
							yield return item;
					} while(r.NextResult());
			}
		}

		public List<TOutput> ReadWithRetry<TOutput>(RetryStrategy retry) {
			for(var n = 1;; ++n) { 
				try { 
					var r = new List<TOutput>();
					r.AddRange(Read<TOutput>());
					return r;
				} catch(Exception e) {
					var again = retry(n, e);
					if(!again.HasValue)
						throw;
					Thread.Sleep(again.Value);
				}
			}
		}

		public TOutput Single<TOutput>() where TOutput : new() =>
			Read<TOutput>().Single();
	}
}
