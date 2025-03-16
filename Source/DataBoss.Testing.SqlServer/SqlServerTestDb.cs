using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using Microsoft.Data.SqlClient;
using DataBoss.Data;
using DataBoss.Linq;
using DataBoss.Data.MsSql;

namespace DataBoss.Testing.SqlServer
{
	public class SqlServerTestDb : IDisposable
	{
		static readonly ConcurrentDictionary<string, SqlServerTestDb> DatabaseInstances = new();

		static string ApplicationName => typeof(SqlServerTestDb).FullName;

		readonly TestDbConfig config;

		public string ConnectionString => config.ToString();
		public string Name => config.Name;

		SqlServerTestDb(TestDbConfig config) {
			this.config = config;
		}

		public void Dispose() =>
			ForceDropDatabase(config);

		public List<SqlServerTestDbSessionInfo> GetActiveSessions() {
			ClearPool(config.GetConnectionString().ToString());
			using var c = SqlCommandExtensions.Open(GetServerConnectionString(config));
			return c.ExecuteQuery<SqlServerTestDbSessionInfo>(
				  "select *\n"
				+ "from sys.dm_exec_sessions s\n"
				+ "cross apply(\n"
				+ "  select * from sys.dm_exec_input_buffer(session_id, null)\n"
				+ ") cmd\n"
				+ "where database_id = db_id(@db)\n"
				+ "and is_user_process = 1",
				new { db = Name }).ToList();
		}

		public static IEnumerable<TestDbInfo> GetAllInstances(string serverConnectionString) {
			using var c = SqlCommandExtensions.Open(new SqlConnectionStringBuilder(serverConnectionString) {
				Pooling = false,
				ApplicationName = ApplicationName,
			}.ToString());


			var xs = c.ExecuteQuery<(int DatabaseId, string Name)>(
				  "select item1 = database_id, item2 = quotename(name)\n"
				+ "from sys.databases\n"
				+ "where name not in('master', 'tempdb', 'model', 'msdb')")
				.ToList();

			return xs.Any()
			? c.ExecuteQuery<TestDbInfo>(
					"select name = db_name, createdAt = cast(value as datetime) from (\n"
					+ string.Join("\t\tunion all\n", xs.Select(x =>
						  $"\tselect db_name = db_name({x.DatabaseId}), class, name, value\n"
						+ $"\tfrom {x.Name}.sys.extended_properties\n"))
					+ ") props\n"
					+ "where class = 0 and name = 'testdb_created_at'")
					.Select(x => new TestDbInfo(
						x.Name,
						DateTime.SpecifyKind(x.CreatedAt, DateTimeKind.Utc).ToLocalTime(),
						new SqlConnectionStringBuilder(serverConnectionString) { InitialCatalog = x.Name }.ToString()))
					.ToList()
			: Enumerable.Empty<TestDbInfo>();
		}

		public static SqlServerTestDb Create(TestDbConfig config = null) =>
			GetTemporaryInstance(config);

		public static SqlServerTestDb GetOrCreate(string name) =>
			GetOrCreate(new TestDbConfig { Name = name });

		public static SqlServerTestDb GetOrCreate(TestDbConfig config) {
			if (config?.Name == null)
				throw new ArgumentNullException("config.Name");
			return GetTemporaryInstance(config);
		}

		static SqlServerTestDb GetTemporaryInstance(TestDbConfig config) {
			config = TestDbConfig.Finalize(config);
			return DatabaseInstances.GetOrAdd(config.Name, delegate {
				lock (DatabaseInstances) {
					if (DatabaseInstances.TryGetValue(config.Name, out var found))
						return found;
					return CreateCleanInstance(config);
				}
			});
		}

		static SqlServerTestDb CreateCleanInstance(TestDbConfig config) {
			ExecuteServerCommands(config, cmd => {
				cmd.AddParameters(new { db = config.Name });
				var found = (int)cmd.ExecuteScalar("select case when exists(select null from sys.databases where name = @db) then 1 else 0 end");
				if (found == 1)
					ForceDropDatabase(config);
				cmd.ExecuteNonQuery(
					  "declare @sql nvarchar(max) = replace('\n"
					+ "create database {0};\n"
					+ "alter database {0} set recovery simple;\n"
					+ "declare @now datetime = getutcdate();\n"
					+ "exec {0}..sp_addextendedproperty @name=''testdb_created_at'', @value=@now;', '{0}', quotename(@db))\n"
					+ "exec(@sql)");
			});
			return new SqlServerTestDb(config);
		}

		public static void DeleteInstances() {
			foreach (var item in DatabaseInstances.Values.ToArray())
				item.Dispose();
		}

		static void ForceDropDatabase(TestDbConfig config) {
			if (DatabaseInstances.TryRemove(config.Name, out var found))
				ForceDropDatabase(found.ConnectionString);
		}

		public static void ForceDropDatabase(string connectionString) {
			using var c = SqlCommandExtensions.Open(connectionString);
			ClearPool(c.Connection.ConnectionString);
			c.CommandText = string.Format(
				  "alter database {0} set single_user with rollback immediate;\n"
				+ "use master;\n"
				+ "drop database {0}", c.ExecuteScalar("select quotename(@db)", new { db = c.Connection.Database }));
			c.ExecuteNonQuery();
		}

		public static void ClearPool(string connectionString) => PoolCleaner(connectionString);

		static readonly Action<string> PoolCleaner = CreatePoolCleaner();

		static Action<string> CreatePoolCleaner() {
			var cs = Expression.Parameter(typeof(string), "connectionString");
			var body = Array.ConvertAll(new[] {
				"Microsoft.Data.SqlClient.SqlConnection, Microsoft.Data.SqlClient, PublicKeyToken=23ec7fc2d6eaa4a5",
				"System.Data.SqlClient.SqlConnection, System.Data.SqlClient, PublicKeyToken=b03f5f7f11d50a3a",
			}, Type.GetType).Where(x => x != null).Select(dbType => {
				var db = Expression.Variable(dbType, "db");
				return Expression.Block(
					new[] { db },
					Expression.Assign(db, Expression.New(dbType.GetConstructor(new[] { typeof(string) }), cs)),
					Expression.Call(null, dbType.GetMethod("ClearPool"), db),
					Expression.Call(db, typeof(IDisposable).GetMethod("Dispose")));
			});

			return Expression.Lambda<Action<string>>(Expression.Block(body), cs).Compile();
		}

		public static void RegisterForAutoCleanup() =>
			AppDomain.CurrentDomain.DomainUnload += delegate { DeleteInstances(); };

		static void ExecuteServerCommands(TestDbConfig config, Action<SqlCommand> execute) {
			var cmd = new SqlCommand {
				Connection = GetServerConnection(config),
			};
			try {
				cmd.Connection.Open();
				execute(cmd);
			}
			finally {
				cmd.Connection.Dispose();
				cmd.Dispose();
			}
		}

		static string GetServerConnectionString(TestDbConfig config) {
			var cs = config.GetServerConnectionString();
			cs.ApplicationName = ApplicationName;
			cs.Pooling = false;
			return cs.ToString();
		}

		static SqlConnection GetServerConnection(TestDbConfig config) =>
			new(GetServerConnectionString(config));
	}
}
