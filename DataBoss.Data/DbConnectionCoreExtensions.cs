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

		static readonly ConcurrentBag<LambdaExpression> connectionWrapers = new ConcurrentBag<LambdaExpression>();

		public static void Register<T>(Expression<Func<T, IDataBossConnection>> wrap) =>
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

			var sql = Type.GetType("DataBoss.Data.DataBossSqlConnection, DataBoss.Data.SqlClient");
			var ctor = sql.GetConstructors().Single();

			var asTarget = Expression.TypeAs(con, ctor.GetParameters()[0].ParameterType);

			WrapConnection = Expression.Lambda<Func<IDbConnection, IDataBossConnection>>(
				Expression.Condition(
					Expression.ReferenceEqual(asTarget, Expression.Constant(null)),
						Expression.Constant(null, typeof(IDataBossConnection)),
						Expression.New(ctor, asTarget)), con)
				.Compile();

		}
	}
}
