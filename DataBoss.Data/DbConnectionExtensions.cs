using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace DataBoss.Data
{
	public static class DbConnectionExtensions
	{
		static MethodInfo AddTo = typeof(ToParams).GetMethod(nameof(ToParams.AddTo));

		static ConcurrentDictionary<Type, Action<IDbCommand, object>> CommandFactory = new ConcurrentDictionary<Type, Action<IDbCommand, object>>();

		public static IDbCommand CreateCommand(this IDbConnection connection, string commandText) {
			var c = connection.CreateCommand();
			c.CommandText = commandText;
			return c;
		}

		public static IDbCommand CreateCommand<T>(this IDbConnection connection, string cmdText, T args) {
			var cmd = CreateCommand(connection, cmdText);
			ToParams.AddTo(cmd, args);
			return cmd;
		}

		public static object ExecuteScalar(this IDbConnection connection, string commandText) =>
			CreateCommand(connection, commandText).Use(DbOps<IDbCommand, IDataReader>.ExecuteScalar);

		public static object ExecuteScalar<T>(this IDbConnection connection, string commandText, T args) =>
			CreateCommand(connection, commandText, args).Use(DbOps<IDbCommand, IDataReader>.ExecuteScalar);

		public static int ExecuteNonQuery(this IDbConnection connection, string commandText) =>
			CreateCommand(connection, commandText).Use(DbOps<IDbCommand, IDataReader>.ExecuteQuery);

		public static int ExecuteNonQuery<T>(this IDbConnection connection, string commandText, T args) =>
			CreateCommand(connection, commandText, args).Use(DbOps<IDbCommand, IDataReader>.ExecuteQuery);

		public static void Into<T>(this IDbConnection connection, string destinationTable, IEnumerable<T> rows) =>
			Into(connection, destinationTable, rows, new DataBossBulkCopySettings());
		
		public static void Into<T>(this IDbConnection connection, string destinationTable, IEnumerable<T> rows, DataBossBulkCopySettings settings) {
			switch(connection) {
				case SqlConnection con: 
					SqlConnectionExtensions.Into(con, destinationTable, rows, settings);
					break;
				case ProfiledSqlConnection con:
					ProfiledSqlConnection.Into(con, destinationTable, SequenceDataReader.Create(rows, x => x.MapAll()), settings);
					break;
				default: throw new NotSupportedException();
			}
		}

		public static IEnumerable<T> Query<T>(this IDbConnection db, string sql, object args = null, bool buffered = true) {
			var q = DbObjectQuery.Create(() => {
				var cmd = db.CreateCommand(sql);
				if (args != null)
					AddParameters(args.GetType())(cmd, args);
				return cmd;
			});
			var rows = q.Read<T>();
			return buffered ? rows.ToList().AsEnumerable() : rows;
		}

		static Action<IDbCommand, object> AddParameters(Type t) =>
			CommandFactory.GetOrAdd(t, type => {
				var db = Expression.Parameter(typeof(IDbCommand));
				var p = Expression.Parameter(typeof(object));
				return Expression.Lambda<Action<IDbCommand, object>>(
					Expression.Call(AddTo.MakeGenericMethod(typeof(IDbCommand), type),
						db, Expression.Convert(p, type)), db, p)
				.Compile();
			});
	}
}