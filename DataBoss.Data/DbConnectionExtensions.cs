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
		
		public static void Into<T>(this IDbConnection connection, string destinationTable, IEnumerable<T> rows, DataBossBulkCopySettings settings) =>
			Into(connection, destinationTable, SequenceDataReader.Create(rows, x => x.MapAll()), settings);

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

		static IDbConnectionExtras GetExtras(IDbConnection connection) {
			switch (connection) {
				case SqlConnection con: return new SqlConnectionExtras(con);
				case IDbConnectionExtras con: return con;
				default: throw new NotSupportedException();
			}
		}

		class SqlConnectionExtras : IDbConnectionExtras
		{
			readonly SqlConnection connection;

			public SqlConnectionExtras(SqlConnection connection) { this.connection = connection; }

			public void CreateTable(string destinationTable, IDataReader data) =>
				connection.CreateTable(destinationTable, data);

			public void Insert(string destinationTable, IDataReader rows, DataBossBulkCopySettings settings) =>
				connection.Insert(destinationTable, rows, settings);
		}

		public static IEnumerable<T> Query<T>(this IDbConnection db, string sql, object args = null, bool buffered = true) => 
			Query<T>(db, sql, new DataBossQueryOptions {
				Parameters = args,
				Buffered = buffered,
			});

		public static IEnumerable<T> Query<T>(this IDbConnection db, string commandText, DataBossQueryOptions options) =>
			new DbQuery {
				Connection = db,
				CommandText = commandText,
				Options = options,
			}.Read<T>();

		class DbQuery
		{
			public IDbConnection Connection;
			public string CommandText;
			public DataBossQueryOptions Options;

			IDbCommand GetCommand() {
				var cmd = Connection.CreateCommand(CommandText);
				cmd.CommandType = Options.CommandType;
				if (Options.CommandTimeout.HasValue)
					cmd.CommandTimeout = Options.CommandTimeout.Value;
				if (Options.Parameters != null)
					AddParameters(Options.Parameters.GetType())(cmd, Options.Parameters);
				return cmd;
			}
	
			public IEnumerable<T> Read<T>() {
				var rows = DbObjectQuery.Create(GetCommand).Read<T>();
				return Options.Buffered ? rows.ToList() as IEnumerable<T> : rows;
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

		public static IDbConnection WithCommandTimeout(this IDbConnection db, int commandTimeout) =>
			new DbConnectionDecorator(db) { CommandTimeout = commandTimeout };
		
		class DbConnectionDecorator : IDbConnection, IDbConnectionExtras
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
		}
	}
}