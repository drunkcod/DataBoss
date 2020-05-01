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
	using System.Collections.Concurrent;
	using System.Collections.Generic;
	using System.Data;
	using System.Linq;
	using System.Linq.Expressions;
	using System.Reflection;

	public static class DbConnectionExtensions
	{
		static MethodInfo AddTo = typeof(ToParams).GetMethod(nameof(ToParams.AddTo));

		static ConcurrentDictionary<Type, Action<IDbCommand, object>> CommandFactory = new ConcurrentDictionary<Type, Action<IDbCommand, object>>();

		static IDataBossConnection GetExtras(IDbConnection connection) =>
			DbConnectionCoreExtensions.GetExtras(connection);
		
		public static IDbCommand CreateCommand<T>(this IDbConnection connection, string cmdText, T args) =>
			GetExtras(connection).CreateCommand(cmdText, args);

		public static object ExecuteScalar<T>(this IDbConnection connection, string commandText, T args) =>
			CreateCommand(connection, commandText, args).Use(DbOps<IDbCommand, IDataReader>.ExecuteScalar);

		public static int ExecuteNonQuery<T>(this IDbConnection connection, string commandText, T args) =>
			CreateCommand(connection, commandText, args).Use(DbOps<IDbCommand, IDataReader>.ExecuteNonQuery);

		public static void Into<T>(this IDbConnection connection, string destinationTable, IEnumerable<T> rows) =>
			Into(connection, destinationTable, rows, new DataBossBulkCopySettings());

		public static void Into<T>(this IDbConnection connection, string destinationTable, IEnumerable<T> rows, DataBossBulkCopySettings settings) =>
			Into(connection, destinationTable, SequenceDataReader.Create(rows, x => x.MapAll()), settings);

		public static void Into(this IDbConnection connection, string destinationTable, IDataReader rows) =>
			Into(connection, destinationTable, rows, new DataBossBulkCopySettings());

		public static void Into(this IDbConnection connection, string destinationTable, IDataReader rows, DataBossBulkCopySettings settings) {
			var extras = GetExtras(connection);
			extras.CreateTable(destinationTable, rows);
			extras.Insert(destinationTable, rows, settings);
		}

		public static void Insert<T>(this IDbConnection connection, string destinationTable, IEnumerable<T> rows) =>
			Insert(connection, destinationTable, rows, new DataBossBulkCopySettings());

		public static void Insert<T>(this IDbConnection connection, string destinationTable, IEnumerable<T> rows, DataBossBulkCopySettings settings) =>
			Insert(connection, destinationTable, SequenceDataReader.Create(rows, x => x.MapAll()), settings);

		public static void Insert(this IDbConnection connection, string destinationTable, IDataReader rows, DataBossBulkCopySettings settings) =>
			GetExtras(connection).Insert(destinationTable, rows, settings);

		public static void CreateTable(this IDbConnection connection, string tableName, IDataReader data) =>
			GetExtras(connection).CreateTable(tableName, data);

		public static IEnumerable<T> Query<T>(this IDbConnection db, string sql, object args = null, bool buffered = true) => Query<T>(db, sql, new DataBossQueryOptions { Parameters = args, Buffered = buffered });
		public static IEnumerable<TResult> Query<T, TResult>(this IDbConnection db, Func<T, TResult> selector, string sql, object args = null, bool buffered = true) => Query(db, sql, args, buffered).Read(selector);
		public static IEnumerable<TResult> Query<T1, T2, TResult>(this IDbConnection db, Func<T1, T2, TResult> selector, string sql, object args = null, bool buffered = true) => Query(db, sql, args, buffered).Read(selector);
		public static IEnumerable<TResult> Query<T1, T2, T3, TResult>(this IDbConnection db, Func<T1, T2, T3, TResult> selector, string sql, object args = null, bool buffered = true) => Query(db, sql, args, buffered).Read(selector);
		public static IEnumerable<TResult> Query<T1, T2, T3, T4, TResult>(this IDbConnection db, Func<T1, T2, T3, T4, TResult> selector, string sql, object args = null, bool buffered = true) => Query(db, sql, args, buffered).Read(selector);
		public static IEnumerable<TResult> Query<T1, T2, T3, T4, T5, TResult>(this IDbConnection db, Func<T1, T2, T3, T4, T5, TResult> selector, string sql, object args = null, bool buffered = true) => Query(db, sql, args, buffered).Read(selector);
		public static IEnumerable<TResult> Query<T1, T2, T3, T4, T5, T6, TResult>(this IDbConnection db, Func<T1, T2, T3, T4, T5, T6, TResult> selector, string sql, object args = null, bool buffered = true) => Query(db, sql, args, buffered).Read(selector);
		public static IEnumerable<TResult> Query<T1, T2, T3, T4, T5, T6, T7, TResult>(this IDbConnection db, Func<T1, T2, T3, T4, T5, T6, T7, TResult> selector, string sql, object args = null, bool buffered = true) => Query(db, sql, args, buffered).Read(selector);

		public static IEnumerable<T> Query<T>(this IDbConnection db, string commandText, DataBossQueryOptions options) => new DbQuery(db, commandText, options).Read<T>();
		public static IEnumerable<TResult> Query<T1, TResult>(this IDbConnection db, Func<T1, TResult> selector, string commandText, DataBossQueryOptions options) => new DbQuery(db, commandText, options).Read(selector);
		public static IEnumerable<TResult> Query<T1, T2, TResult>(this IDbConnection db, Func<T1, T2, TResult> selector, string commandText, DataBossQueryOptions options) => new DbQuery(db, commandText, options).Read(selector);
		public static IEnumerable<TResult> Query<T1, T2, T3, TResult>(this IDbConnection db, Func<T1, T2, T3, TResult> selector, string commandText, DataBossQueryOptions options) => new DbQuery(db, commandText, options).Read(selector);
		public static IEnumerable<TResult> Query<T1, T2, T3, T4, TResult>(this IDbConnection db, Func<T1, T2, T3, T4, TResult> selector, string commandText, DataBossQueryOptions options) => new DbQuery(db, commandText, options).Read(selector);
		public static IEnumerable<TResult> Query<T1, T2, T3, T4, T5, TResult>(this IDbConnection db, Func<T1, T2, T3, T4, T5, TResult> selector, string commandText, DataBossQueryOptions options) => new DbQuery(db, commandText, options).Read(selector);
		public static IEnumerable<TResult> Query<T1, T2, T3, T4, T5, T6, TResult>(this IDbConnection db, Func<T1, T2, T3, T4, T5, T6, TResult> selector, string commandText, DataBossQueryOptions options) => new DbQuery(db, commandText, options).Read(selector);
		public static IEnumerable<TResult> Query<T1, T2, T3, T4, T5, T6, T7, TResult>(this IDbConnection db, Func<T1, T2, T3, T4, T5, T6, T7, TResult> selector, string commandText, DataBossQueryOptions options) => new DbQuery(db, commandText, options).Read(selector);

		static DbQuery Query(IDbConnection db, string commandText, object args, bool buffered) =>
			new DbQuery(db, commandText, new DataBossQueryOptions { Parameters = args, Buffered = buffered });

		class DbQuery
		{
			public readonly IDbConnection Connection;
			public readonly string CommandText;
			public readonly DataBossQueryOptions Options;

			public DbQuery(IDbConnection db, string command, DataBossQueryOptions options) {
				this.Connection = db;
				this.CommandText = command;
				this.Options = options;
			}

			IDbCommand GetCommand() {
				var cmd = Connection.CreateCommand(CommandText);
				cmd.CommandType = Options.CommandType == 0 ? CommandType.Text : Options.CommandType;
				if (Options.CommandTimeout.HasValue)
					cmd.CommandTimeout = Options.CommandTimeout.Value;
				if (Options.Parameters != null)
					AddParameters(Options.Parameters.GetType())(cmd, Options.Parameters);
				return cmd;
			}
	
			public IEnumerable<T> Read<T>() => BufferOrNot(DbObjectQuery.Create(GetCommand).Read<T>());

			public IEnumerable<TResult> Read<T1, TResult>(Func<T1, TResult> selector) => BufferOrNot(DbObjectQuery.Create(GetCommand).Read(selector));
			public IEnumerable<TResult> Read<T1, T2, TResult>(Func<T1, T2, TResult> selector) => BufferOrNot(DbObjectQuery.Create(GetCommand).Read(selector));
			public IEnumerable<TResult> Read<T1, T2, T3, TResult>(Func<T1, T2, T3, TResult> selector) => BufferOrNot(DbObjectQuery.Create(GetCommand).Read(selector));
			public IEnumerable<TResult> Read<T1, T2, T3, T4, TResult>(Func<T1, T2, T3, T4, TResult> selector) => BufferOrNot(DbObjectQuery.Create(GetCommand).Read(selector));
			public IEnumerable<TResult> Read<T1, T2, T3, T4, T5, TResult>(Func<T1, T2, T3, T4, T5, TResult> selector) => BufferOrNot(DbObjectQuery.Create(GetCommand).Read(selector));
			public IEnumerable<TResult> Read<T1, T2, T3, T4, T5, T6, TResult>(Func<T1, T2, T3, T4, T5, T6, TResult> selector) => BufferOrNot(DbObjectQuery.Create(GetCommand).Read(selector));
			public IEnumerable<TResult> Read<T1, T2, T3, T4, T5, T6, T7, TResult>(Func<T1, T2, T3, T4, T5, T6, T7, TResult> selector) => BufferOrNot(DbObjectQuery.Create(GetCommand).Read(selector));

			IEnumerable<T> BufferOrNot<T>(IEnumerable<T> xs) => Options.Buffered ? xs.ToList() : xs;

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

		public static IDbConnection WithCommandTimeout(this IDbConnection db, int commandTimeout) =>
			new DbConnectionDecorator(db) { CommandTimeout = commandTimeout };
		
		class DbConnectionDecorator : IDbConnection, IDataBossConnection
		{
			 readonly IDbConnection InnerConnection;

			public DbConnectionDecorator(IDbConnection inner) { this.InnerConnection = inner; }

			public int? CommandTimeout;

			public string ConnectionString { 
				get => InnerConnection.ConnectionString; 
				set => InnerConnection.ConnectionString = value; 
			}

			public int ConnectionTimeout => InnerConnection.ConnectionTimeout;

			public string Database => InnerConnection.Database;

			public ConnectionState State => InnerConnection.State;

			public IDbTransaction BeginTransaction() => InnerConnection.BeginTransaction();
			public IDbTransaction BeginTransaction(IsolationLevel il) => InnerConnection.BeginTransaction(il);

			public void ChangeDatabase(string databaseName) => InnerConnection.ChangeDatabase(databaseName);

			public void Close() => InnerConnection.Close();

			public IDbCommand CreateCommand() {
				var c = InnerConnection.CreateCommand();
				if(CommandTimeout.HasValue)
					c.CommandTimeout = CommandTimeout.Value;
				return c;
			}

			public void Dispose() => InnerConnection.Dispose();

			public void Open() => InnerConnection.Open();

			public void CreateTable(string destinationTable, IDataReader rows) =>
				InnerConnection.CreateTable(destinationTable, rows);

			public void Insert(string destinationTable, IDataReader rows, DataBossBulkCopySettings settings) => 
				InnerConnection.Insert(destinationTable, rows, settings.CommandTimeout.HasValue ? settings : settings.WithCommandTimeout(CommandTimeout));

			public IDbCommand CreateCommand<T>(string cmdText, T args) =>
				InnerConnection.CreateCommand(cmdText, args);
		}
	}
}