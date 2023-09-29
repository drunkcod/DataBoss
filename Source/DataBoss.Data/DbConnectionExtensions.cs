namespace DataBoss.Data
{
	using System;
	using System.Collections.Concurrent;
	using System.Collections.Generic;
	using System.Data;
	using System.Data.Common;
	using System.Linq;
	using System.Linq.Expressions;
	using System.Threading;
	using System.Threading.Tasks;

	public static class DbConnectionExtensions
	{
		static Func<IDbConnection, IDataBossConnection> WrapConnection = _ => null;
		static readonly ConcurrentStack<Delegate> connectionWrapers = new();

		public static void Register<T>(Func<T, IDataBossConnection> wrap) =>
			connectionWrapers.Push(wrap);

		public static void DisposeOnClose(this DbConnection connection) {
			connection.StateChange += DisposeOnCloseHandler;
		}

		static readonly StateChangeEventHandler DisposeOnCloseHandler = (obj, e) => {
			if (e.CurrentState == ConnectionState.Closed) {
				var c = (DbConnection)obj;
				c.StateChange -= DisposeOnCloseHandler;
				c.Dispose();
			}
		};

		public static IDbCommand CreateCommand(this IDbConnection connection, string commandText) {
			var c = connection.CreateCommand();
			c.CommandText = commandText;
			return c;
		}

		public static IDbCommand CreateCommand<T>(this IDbConnection connection, string cmdText, T args) =>
			Wrap(connection).CreateCommand(cmdText, args);

		public static IDbCommand CreateCommand(this IDbConnection connection, string cmdText, object args) =>
			Wrap(connection).CreateCommand(cmdText, args);

		public static object ExecuteScalar<T>(this IDbConnection connection, string commandText, T args) {
			using var c = CreateCommand(connection, commandText, args);
			return c.ExecuteScalar();
		}

		public static object ExecuteScalar(this IDbConnection connection, string commandText) {
			using var c = CreateCommand(connection, commandText);
			return c.ExecuteScalar();
		}

		public static int ExecuteNonQuery<T>(this IDbConnection connection, string commandText, T args) {
			using var c = CreateCommand(connection, commandText, args);
			return c.ExecuteNonQuery();
		}

		public static int ExecuteNonQuery(this IDbConnection connection, string commandText) {
			using var c = CreateCommand(connection, commandText);
			return c.ExecuteNonQuery();
		}

		public static void Into<T>(this IDbConnection connection, string destinationTable, IEnumerable<T> rows) =>
			Into(connection, destinationTable, rows, new DataBossBulkCopySettings());

		public static void Into<T>(this IDbConnection connection, string destinationTable, IEnumerable<T> rows, DataBossBulkCopySettings settings) =>
			Into(connection, destinationTable, SequenceDataReader.Create(rows, x => x.MapAll()), settings);

		public static void Into(this IDbConnection connection, string destinationTable, IDataReader rows) =>
			Into(connection, destinationTable, rows, new DataBossBulkCopySettings());

		public static void Into(this IDbConnection connection, string destinationTable, IDataReader rows, DataBossBulkCopySettings settings) {
			var extras = Wrap(connection);
			extras.CreateTable(destinationTable, rows);
			extras.Insert(destinationTable, rows, settings);
		}

		public static void Insert<T>(this IDbConnection connection, string destinationTable, IEnumerable<T> rows) =>
			Insert(connection, destinationTable, rows, new DataBossBulkCopySettings());

		public static void Insert<T>(this IDbConnection connection, string destinationTable, IEnumerable<T> rows, DataBossBulkCopySettings settings) =>
			Insert(connection, destinationTable, SequenceDataReader.Create(rows, x => x.MapAll()), settings);

		public static void Insert(this IDbConnection connection, string destinationTable, IDataReader rows, DataBossBulkCopySettings settings) =>
			Wrap(connection).Insert(destinationTable, rows, settings);

		public static Task InsertAsync<T>(this IDbConnection connection, string destinationTable, IAsyncEnumerable<T> rows, CancellationToken cancellationToken = default) =>
			InsertAsync(connection, destinationTable, rows, new DataBossBulkCopySettings(), cancellationToken);

		public static Task InsertAsync<T>(this IDbConnection connection, string destinationTable, IAsyncEnumerable<T> rows, DataBossBulkCopySettings settings, CancellationToken cancellationToken = default) =>
			InsertAsync(connection, destinationTable, SequenceDataReader.Create(rows, x => x.MapAll()), settings, cancellationToken);

		public static Task InsertAsync(this IDbConnection connection, string destinationTable, DbDataReader rows, DataBossBulkCopySettings settings, CancellationToken cancellationToken = default) =>
			Wrap(connection).InsertAsync(destinationTable, rows, settings, cancellationToken);

		public static void CreateTable(this IDbConnection connection, string tableName, IDataReader data) =>
			Wrap(connection).CreateTable(tableName, data);

		public static IEnumerable<T> Query<T>(this IDbConnection db, string sql) => Query<T>(db, sql, new DataBossQueryOptions { Parameters = null, Buffered = true });
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
			new(db, commandText, new DataBossQueryOptions { Parameters = args, Buffered = buffered });

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

			public IEnumerable<T> Read<T>() => DbObjectQuery.Create(GetCommand, Options.Buffered).Read<T>();

			public IEnumerable<TResult> Read<T1, TResult>(Func<T1, TResult> selector) => DbObjectQuery.Create(GetCommand, Options.Buffered).Read<Func<T1, TResult>, TResult>(selector);
			public IEnumerable<TResult> Read<T1, T2, TResult>(Func<T1, T2, TResult> selector) => DbObjectQuery.Create(GetCommand, Options.Buffered).Read<Func<T1, T2, TResult>, TResult>(selector);
			public IEnumerable<TResult> Read<T1, T2, T3, TResult>(Func<T1, T2, T3, TResult> selector) => DbObjectQuery.Create(GetCommand, Options.Buffered).Read<Func<T1, T2, T3, TResult>, TResult>(selector);
			public IEnumerable<TResult> Read<T1, T2, T3, T4, TResult>(Func<T1, T2, T3, T4, TResult> selector) => DbObjectQuery.Create(GetCommand, Options.Buffered).Read<Func<T1, T2, T3, T4, TResult>, TResult>(selector);
			public IEnumerable<TResult> Read<T1, T2, T3, T4, T5, TResult>(Func<T1, T2, T3, T4, T5, TResult> selector) => DbObjectQuery.Create(GetCommand, Options.Buffered).Read<Func<T1, T2, T3, T4, T5, TResult>, TResult>(selector);
			public IEnumerable<TResult> Read<T1, T2, T3, T4, T5, T6, TResult>(Func<T1, T2, T3, T4, T5, T6, TResult> selector) => DbObjectQuery.Create(GetCommand, Options.Buffered).Read<Func<T1, T2, T3, T4, T5, T6, TResult>, TResult>(selector);
			public IEnumerable<TResult> Read<T1, T2, T3, T4, T5, T6, T7, TResult>(Func<T1, T2, T3, T4, T5, T6, T7, TResult> selector) => DbObjectQuery.Create(GetCommand, Options.Buffered).Read<Func<T1, T2, T3, T4, T5, T6, T7, TResult>, TResult>(selector);
		}

		static readonly ConcurrentDictionary<Type, Action<IDbCommand, object>> CommandFactory = new();

		internal static Action<IDbCommand, object> AddParameters(Type t, ISqlDialect dialect) =>
			CommandFactory.GetOrAdd(t,
				type => (Action<IDbCommand, object>)ToParams.CreateExtractor(dialect, typeof(IDbCommand), type, typeof(object)).Compile());

		public static IDbConnection WithCommandTimeout(this IDbConnection db, int commandTimeout) =>
			new DbConnectionDecorator(db) { CommandTimeout = commandTimeout };

		class DbConnectionDecorator  : IDbConnection, IDataBossConnection
		{
			readonly IDbConnection InnerConnection;
			readonly IDataBossConnection DataBossConnection;

			public DbConnectionDecorator(IDbConnection inner) {
				this.InnerConnection = inner;
				this.DataBossConnection = Wrap(inner);
			}

			public ISqlDialect Dialect => DataBossConnection.Dialect;
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
				DataBossConnection.CreateTable(destinationTable, rows);

			public void Insert(string destinationTable, IDataReader rows, DataBossBulkCopySettings settings) =>
				DataBossConnection.Insert(destinationTable, rows, settings.CommandTimeout.HasValue ? settings : settings.WithCommandTimeout(CommandTimeout));

			public Task InsertAsync(string destinationTable, DbDataReader rows, DataBossBulkCopySettings settings, CancellationToken cancellationToken = default) =>
				DataBossConnection.InsertAsync(destinationTable, rows, settings.CommandTimeout.HasValue ? settings : settings.WithCommandTimeout(CommandTimeout), cancellationToken);

			public IDbCommand CreateCommand() =>
				Adorn(InnerConnection.CreateCommand());

			public IDbCommand CreateCommand(string cmdText) =>
				Adorn(DataBossConnection.CreateCommand(cmdText));

			public IDbCommand CreateCommand<T>(string cmdText, T args) =>
				Adorn(DataBossConnection.CreateCommand(cmdText, args));

			public IDbCommand CreateCommand(string cmdText, object args) =>
				Adorn(DataBossConnection.CreateCommand(cmdText, args));

			public void EnsureDatabase() => DataBossConnection.EnsureDatabase();
			public int GetTableVersion(string tableName) => DataBossConnection.GetTableVersion(tableName);
			public void SetTableVersion(string tableName, int version) => DataBossConnection.SetTableVersion(tableName, version);
			public string GetDefaultSchema() => DataBossConnection.GetDefaultSchema();

			IDbCommand Adorn(IDbCommand c) {
				if (CommandTimeout.HasValue)
					c.CommandTimeout = CommandTimeout.Value;
				return c;
			}
		}

		public static IDataBossConnection Wrap(IDbConnection connection) {			
			var x = WrapConnection(connection) ?? connection as IDataBossConnection;
			if(x != null)
				return x;
			
			BuildWrapperFactory();
			return WrapConnection(connection) ?? throw new NotSupportedException($"Failed to wrap {connection.GetType().FullName} missing NuGet reference to DataBoss.Data.SqlClient or DataBoss.Data.MsSql?");
		}

		static void BuildWrapperFactory()
		{
			var con = Expression.Parameter(typeof(IDbConnection), "c");

			Expression body = Expression.Constant(null, typeof(IDataBossConnection));
			foreach(var item in new [] {
				"DataBoss.Data.MsSql.DataBossSqlConnection, DataBoss.Data.MsSql",
				"DataBoss.Data.DataBossSqlConnection, DataBoss.Data.SqlClient",
				"DataBoss.Data.Npgsql.DataBossNpgsqlConnection, DataBoss.Data.Npgsql"}) {

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

			foreach(var item in connectionWrapers) {
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

	public static class DbCommandExtensions
	{
		public static IEnumerator<T> ExecuteQuery<T>(this IDbCommand self, string query) {
			self.CommandText = query;
			return ExecuteQuery<T>(self);
		}
	
		public static IEnumerator<T> ExecuteQuery<T>(this IDbCommand self, string query, object args) {
			self.CommandText = query;
			self.Parameters.Clear();
			if(args != null)
				DbConnectionExtensions.AddParameters(args.GetType(), DbConnectionExtensions.Wrap(self.Connection).Dialect)(self, args);

			return ExecuteQuery<T>(self);
		}

		public static IEnumerator<T> ExecuteQuery<T>(this IDbCommand cmd) {
			var reader = cmd.ExecuteReader();
			var converter = ConverterFactory.Default.GetConverter<IDataReader, T>(reader).Compiled;
			return new ConvertingEnumerator<IDataReader, T>(reader, converter);
		}

		public static int ExecuteNonQuery(this IDbCommand self, string query) {
			self.CommandText = query;
			return self.ExecuteNonQuery();
		}
	}
}
