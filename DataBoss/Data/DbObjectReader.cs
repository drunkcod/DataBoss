using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;

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
				using(var r = q.ExecuteReader()) {
					var materialize = ObjectReader.GetConverter<TOutput>(r);
					do {
						while(r.Read())
							yield return materialize(r);
					} while(r.NextResult());
				}
			}
		}

		public TOutput Single<TOutput>() where TOutput : new() =>
			Read<TOutput>().Single();
	}

	public class DbObjectReader
	{
		readonly SqlConnection db;

		public IEnumerable<T> Read<T>(string command) => Query(command, new {}).Read<T>();

		public T Single<T>(string command) => Read<T>(command).Single();

		public DbObjectQuery Query<T>(string command, T args) =>
			new DbObjectQuery(() => db.CreateCommand(command, args));

		public DbObjectQuery Query<T,T2>(string command, IEnumerable<T> args, Func<T,T2> toArg) =>
			new DbObjectQuery(() => {
				var q = new StringBuilder();
				var cmd = new SqlCommand();
				var format = Regex.Replace(command, @"(@[A-Za-z_]+)", "$1$${0}");

				using(var item = args.GetEnumerator())
					for(var n = 0; item.MoveNext(); ++n) {
						var p = ToParams.Invoke(toArg(item.Current));
						for(var i = 0; i != p.Length; ++i)
							p[i].ParameterName += "$" + n;
						q.AppendFormat(format, n);
						q.AppendLine();
						cmd.Parameters.AddRange(p);
					}
				cmd.Connection = db;
				cmd.CommandText = q.ToString();
				return cmd;
			});

		public DbObjectReader(SqlConnection db) {
			this.db = db;
		}	
	}
}
