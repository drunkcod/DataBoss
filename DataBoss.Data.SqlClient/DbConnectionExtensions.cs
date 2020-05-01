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

	public static class DbConnectionExtensions
	{
		static IDataBossConnection Wrap(IDbConnection db) => DbConnectionCoreExtensions.GetExtras(db);

		static ConcurrentDictionary<Type, Action<IDbCommand, object>> CommandFactory = new ConcurrentDictionary<Type, Action<IDbCommand, object>>();
		
		public static IEnumerable<T> Query<T>(this IDbConnection db, string sql, object args = null, bool buffered = true) => Query<T>(db, sql, new DataBossQueryOptions { Parameters = args, Buffered = buffered });
		public static IEnumerable<TResult> Query<T, TResult>(this IDbConnection db, Func<T, TResult> selector, string sql, object args = null, bool buffered = true) => Query(Wrap(db), sql, args, buffered).Read(selector);
		public static IEnumerable<TResult> Query<T1, T2, TResult>(this IDbConnection db, Func<T1, T2, TResult> selector, string sql, object args = null, bool buffered = true) => Query(Wrap(db), sql, args, buffered).Read(selector);
		public static IEnumerable<TResult> Query<T1, T2, T3, TResult>(this IDbConnection db, Func<T1, T2, T3, TResult> selector, string sql, object args = null, bool buffered = true) => Query(Wrap(db), sql, args, buffered).Read(selector);
		public static IEnumerable<TResult> Query<T1, T2, T3, T4, TResult>(this IDbConnection db, Func<T1, T2, T3, T4, TResult> selector, string sql, object args = null, bool buffered = true) => Query(Wrap(db), sql, args, buffered).Read(selector);
		public static IEnumerable<TResult> Query<T1, T2, T3, T4, T5, TResult>(this IDbConnection db, Func<T1, T2, T3, T4, T5, TResult> selector, string sql, object args = null, bool buffered = true) => Query(Wrap(db), sql, args, buffered).Read(selector);
		public static IEnumerable<TResult> Query<T1, T2, T3, T4, T5, T6, TResult>(this IDbConnection db, Func<T1, T2, T3, T4, T5, T6, TResult> selector, string sql, object args = null, bool buffered = true) => Query(Wrap(db), sql, args, buffered).Read(selector);
		public static IEnumerable<TResult> Query<T1, T2, T3, T4, T5, T6, T7, TResult>(this IDbConnection db, Func<T1, T2, T3, T4, T5, T6, T7, TResult> selector, string sql, object args = null, bool buffered = true) => Query(Wrap(db), sql, args, buffered).Read(selector);

		public static IEnumerable<T> Query<T>(this IDbConnection db, string commandText, DataBossQueryOptions options) => new DbQuery(Wrap(db), commandText, options).Read<T>();
		public static IEnumerable<TResult> Query<T1, TResult>(this IDbConnection db, Func<T1, TResult> selector, string commandText, DataBossQueryOptions options) => new DbQuery(Wrap(db), commandText, options).Read(selector);
		public static IEnumerable<TResult> Query<T1, T2, TResult>(this IDbConnection db, Func<T1, T2, TResult> selector, string commandText, DataBossQueryOptions options) => new DbQuery(Wrap(db), commandText, options).Read(selector);
		public static IEnumerable<TResult> Query<T1, T2, T3, TResult>(this IDbConnection db, Func<T1, T2, T3, TResult> selector, string commandText, DataBossQueryOptions options) => new DbQuery(Wrap(db), commandText, options).Read(selector);
		public static IEnumerable<TResult> Query<T1, T2, T3, T4, TResult>(this IDbConnection db, Func<T1, T2, T3, T4, TResult> selector, string commandText, DataBossQueryOptions options) => new DbQuery(Wrap(db), commandText, options).Read(selector);
		public static IEnumerable<TResult> Query<T1, T2, T3, T4, T5, TResult>(this IDbConnection db, Func<T1, T2, T3, T4, T5, TResult> selector, string commandText, DataBossQueryOptions options) => new DbQuery(Wrap(db), commandText, options).Read(selector);
		public static IEnumerable<TResult> Query<T1, T2, T3, T4, T5, T6, TResult>(this IDbConnection db, Func<T1, T2, T3, T4, T5, T6, TResult> selector, string commandText, DataBossQueryOptions options) => new DbQuery(Wrap(db), commandText, options).Read(selector);
		public static IEnumerable<TResult> Query<T1, T2, T3, T4, T5, T6, T7, TResult>(this IDbConnection db, Func<T1, T2, T3, T4, T5, T6, T7, TResult> selector, string commandText, DataBossQueryOptions options) => new DbQuery(Wrap(db), commandText, options).Read(selector);

		static DbQuery Query(IDataBossConnection db, string commandText, object args, bool buffered) =>
			new DbQuery(db, commandText, new DataBossQueryOptions { Parameters = args, Buffered = buffered });

		class DbQuery
		{
			public readonly IDataBossConnection Connection;
			public readonly string CommandText;
			public readonly DataBossQueryOptions Options;

			public DbQuery(IDataBossConnection db, string command, DataBossQueryOptions options) {
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
					AddParameters(Options.Parameters.GetType(), Connection.Dialect)(cmd, Options.Parameters);
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

			static Action<IDbCommand, object> AddParameters(Type t, ISqlDialect dialect) =>
				CommandFactory.GetOrAdd(t, 
					type => (Action<IDbCommand, object>)ToParams.CreateExtractor(dialect, typeof(IDbCommand), type, typeof(object)).Compile());
		}

		public static IDbConnection WithCommandTimeout(this IDbConnection db, int commandTimeout) =>
			new DbConnectionDecorator(db, Wrap(db).Dialect) { CommandTimeout = commandTimeout };
		
		class DbConnectionDecorator : IDbConnection, IDataBossConnection
		{
			readonly IDbConnection InnerConnection;
			

			public DbConnectionDecorator(IDbConnection inner, ISqlDialect dialect) { 
				this.InnerConnection = inner;
				this.Dialect = dialect;
			}

			public ISqlDialect Dialect { get; }
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


			public void Dispose() => InnerConnection.Dispose();

			public void Open() => InnerConnection.Open();

			public void CreateTable(string destinationTable, IDataReader rows) =>
				InnerConnection.CreateTable(destinationTable, rows);

			public void Insert(string destinationTable, IDataReader rows, DataBossBulkCopySettings settings) => 
				InnerConnection.Insert(destinationTable, rows, settings.CommandTimeout.HasValue ? settings : settings.WithCommandTimeout(CommandTimeout));

			public IDbCommand CreateCommand() => 
				Adorn(InnerConnection.CreateCommand());

			public IDbCommand CreateCommand(string cmdText) =>
				Adorn(InnerConnection.CreateCommand(cmdText));

			public IDbCommand CreateCommand<T>(string cmdText, T args) => 
				Adorn(InnerConnection.CreateCommand(cmdText, args));

			IDbCommand Adorn(IDbCommand c) {
				if (CommandTimeout.HasValue)
					c.CommandTimeout = CommandTimeout.Value;
				return c;
			}
		}
	}
}