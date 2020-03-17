#if MSSQLCLIENT
namespace DataBoss.Data.MsSql
{
	using Microsoft.Data.SqlClient;
#else
namespace DataBoss.Data
{
	using System.Data.SqlClient;
#endif

	using System;
	using System.Collections.Generic;
	using System.Data;
	using System.Linq;
	using System.Text;
	using System.Text.RegularExpressions;

	public static class DbObjectReader
	{
		public static DbObjectReader<SqlCommand, SqlDataReader> Create(SqlConnection connection) => new DbObjectReader<SqlCommand, SqlDataReader>(connection.CreateCommand);
	}

	public class DbObjectReader<TCommand, TReader>
		where TCommand : IDbCommand
		where TReader : IDataReader
	{
		static Regex FormatEx = new Regex(@"(@[A-Za-z_]+)");

		readonly Func<TCommand> newCommand;

		public DbObjectReader(Func<TCommand> newCommand) {
			this.newCommand = newCommand;
		}

		public TimeSpan? CommandTimeout = TimeSpan.FromSeconds(30);

		public IEnumerable<T> Read<T>(string command) => Query(command, new {}).Read<T>();
		public IEnumerable<T> Read<T>(string command, ConverterCollection converters) => Query(command, new { }).Read<T>(converters);

		public T Single<T>(string command) => Read<T>(command).Single();

		public DbObjectQuery<TCommand, TReader> Query<T>(string command, T args) =>
			new DbObjectQuery<TCommand, TReader>(() => {
				var cmd = CreateCommand();
				cmd.CommandType = CommandType.Text;
				cmd.CommandText = command;
				ToParams.AddTo(cmd, args);
				return cmd;
			}, DbOps<TCommand, TReader>.ExecuteReader);

		public DbObjectQuery<TCommand, TReader> Query<T,T2>(string command, IEnumerable<T> args, Func<T,T2> toArg) =>
			new DbObjectQuery<TCommand, TReader>(() => {
				var q = new StringBuilder();
				var cmd = CreateCommand();
				var format = FormatEx.Replace(command, "$1$${0}");
				using(var item = args.GetEnumerator())
					for(var n = 0; item.MoveNext(); ++n) {
						var first = cmd.Parameters.Count;
						ToParams.AddTo(cmd, toArg(item.Current));
						for(var i = first; i != cmd.Parameters.Count; ++i)
							((IDbDataParameter)cmd.Parameters[i]).ParameterName += "$" + n;
						q.AppendFormat(format, n);
						q.AppendLine();
					}
				cmd.CommandText = q.ToString();
				return cmd;
			}, DbOps<TCommand, TReader>.ExecuteReader);

		TCommand CreateCommand() {
			var cmd = newCommand();
			cmd.CommandTimeout = CommandTimeout.HasValue ? (int)CommandTimeout.Value.TotalSeconds : 0;
			return cmd;
		}
	}
}
