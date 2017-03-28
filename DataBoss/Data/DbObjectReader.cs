using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;

namespace DataBoss.Data
{

	public class DbObjectReader
	{
		static Regex FormatEx = new Regex(@"(@[A-Za-z_]+)");

		readonly SqlConnection db;

		public TimeSpan? CommandTimeout = TimeSpan.FromSeconds(30);

		public IEnumerable<T> Read<T>(string command) => Query(command, new {}).Read<T>();

		public T Single<T>(string command) => Read<T>(command).Single();

		public DbObjectQuery Query<T>(string command, T args) =>
			new DbObjectQuery(() => {
				var cmd = CreateCommand();
				cmd.CommandText = command;
				cmd.Parameters.AddRange(ToParams.Invoke(args));
				return cmd;
			});

		public DbObjectQuery Query<T,T2>(string command, IEnumerable<T> args, Func<T,T2> toArg) =>
			new DbObjectQuery(() => {
				var q = new StringBuilder();
				var cmd = CreateCommand();
				var format = FormatEx.Replace(command, "$1$${0}");
				using(var item = args.GetEnumerator())
					for(var n = 0; item.MoveNext(); ++n) {
						var p = ToParams.Invoke(toArg(item.Current));
						for(var i = 0; i != p.Length; ++i)
							p[i].ParameterName += "$" + n;
						q.AppendFormat(format, n);
						q.AppendLine();
						cmd.Parameters.AddRange(p);
					}
				cmd.CommandText = q.ToString();
				return cmd;
			});

		SqlCommand CreateCommand()  =>
			new SqlCommand {
				Connection = db,
				CommandTimeout = CommandTimeout.HasValue ? (int)CommandTimeout.Value.TotalSeconds : 0
			};

		public DbObjectReader(SqlConnection db) {
			this.db = db;
		}	
	}
}
