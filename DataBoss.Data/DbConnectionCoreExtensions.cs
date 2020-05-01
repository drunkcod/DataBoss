namespace DataBoss.Data
{
	using System;
	using System.Collections.Concurrent;
	using System.Data;
	using System.Data.Common;
	using System.Linq;
	using System.Linq.Expressions;

	public static class DbConnectionCoreExtensions
	{
		public static void DisposeOnClose(this DbConnection connection) {
			connection.StateChange += DisposeOnClose;
		}

		static void DisposeOnClose(object obj, StateChangeEventArgs e) {
			if (e.CurrentState == ConnectionState.Closed) {
				var c = (DbConnection)obj;
				c.StateChange -= DisposeOnClose;
				c.Dispose();
			}
		}

		public static IDbCommand CreateCommand(this IDbConnection connection, string commandText) {
			var c = connection.CreateCommand();
			c.CommandText = commandText;
			return c;
		}

		public static object ExecuteScalar(this IDbConnection connection, string commandText) =>
			CreateCommand(connection, commandText).Use(DbOps<IDbCommand, IDataReader>.ExecuteScalar);

		public static int ExecuteNonQuery(this IDbConnection connection, string commandText) =>
			CreateCommand(connection, commandText).Use(DbOps<IDbCommand, IDataReader>.ExecuteNonQuery);

		static readonly ConcurrentBag<Delegate> connectionWrapers = new ConcurrentBag<Delegate>();

		public static void Register<T>(Func<T, IDataBossConnection> wrap) =>
			connectionWrapers.Add(wrap);

		static Func<IDbConnection, IDataBossConnection> WrapConnection = _ => null;

		public static IDataBossConnection GetExtras(IDbConnection connection) {
			var x = WrapConnection(connection);
			if(x != null)
				return x;
			BuildWrapperFactory();
			return WrapConnection(connection) ?? throw new NotSupportedException(connection.GetType().Name);
		}

		static void BuildWrapperFactory()
		{
			var con = Expression.Parameter(typeof(IDbConnection), "c");

			Expression body = Expression.Constant(null, typeof(IDataBossConnection));
			foreach(var item in new[]{
				"DataBoss.Data.MsSqlClient.DataBossSqlConnection, DataBoss.Data.MsSqlClient",
				"DataBoss.Data.DataBossSqlConnection, DataBoss.Data.SqlClient", }) {

				var type = Type.GetType(item);
				if(type == null)
					continue;
				var ctor = type.GetConstructors().Single();
				var asTarget = Expression.TypeAs(con, ctor.GetParameters()[0].ParameterType);
				body = Expression.Condition(
					Expression.ReferenceEqual(asTarget, Expression.Constant(null)), 
					body,
					Expression.Convert(Expression.New(ctor, asTarget), typeof(IDataBossConnection)));
			}

			foreach(var item in connectionWrapers.Reverse()) {
				var asTarget = Expression.TypeAs(con, item.Method.GetParameters().Single().ParameterType);
				body = Expression.Condition(
					Expression.ReferenceEqual(asTarget, Expression.Constant(null)),
					body,
					Expression.Convert(Expression.Call(
						Expression.Constant(item.Target), item.Method, asTarget), typeof(IDataBossConnection)));
			}

			WrapConnection = Expression.Lambda<Func<IDbConnection, IDataBossConnection>>(body, con)
				.Compile();

		}
	}
}
