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

		public TOutput Single<TOutput>() where TOutput : new() =>
			Read<TOutput>().Single();
	}
}
