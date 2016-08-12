using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;

namespace DataBoss.Data
{
	public class DbObjectQuery
	{
		readonly Func<SqlCommand> getCommand;
		readonly ObjectReader reader;

		public DbObjectQuery(Func<SqlCommand> getCommand, ObjectReader reader)
		{
			this.getCommand = getCommand;
			this.reader = reader;
		}

		public IEnumerable<TOutput> Read<TOutput>() where TOutput : new()
		{
			using(var q = getCommand()) {
				if(string.IsNullOrEmpty(q.CommandText))
					yield break;
				using(var r = q.ExecuteReader())
					do {
						foreach(var row in reader.Read<TOutput>(r))
							yield return row;
					} while(r.NextResult());
			}
		}

		public TOutput Single<TOutput>() where TOutput : new() =>
			Read<TOutput>().Single();
	}

	public class DbObjectReader
	{
		readonly ObjectReader reader = new ObjectReader();
		readonly SqlConnection db;

		public IEnumerable<T> Read<T>(string command) where T : new() => Query(command, new {}).Read<T>();

		public T Single<T>(string command) where T : new() => Read<T>(command).Single();

		public DbObjectQuery Query<T>(string command, T args) =>
			new DbObjectQuery(() => db.CreateCommand(command, args), reader);

		public DbObjectQuery Query<T,T2>(string command, IEnumerable<T> args, Func<T,T2> toArg ) {
			Func<SqlCommand> buildCommand = () => {
				var q = new StringBuilder();
				var cmd = new SqlCommand();
				var n = 0;
				var format = Regex.Replace(command, @"(@[A-Za-z_]+)", "$1{0}");
				foreach(var item in args) {
					var p = ToParams.Invoke(toArg(item));
					for(var i = 0; i != p.Length; ++i)
						p[i].ParameterName = p[i].ParameterName + n;
					q.AppendFormat(format, n);
					q.AppendLine();
					++n;
					cmd.Parameters.AddRange(p);
				}
				cmd.Connection = db;
				cmd.CommandText = q.ToString();
				return cmd;
			};
			return new DbObjectQuery(buildCommand, reader);
		}

		public DbObjectReader(SqlConnection db) {
			this.db = db;
		}	
	}}
