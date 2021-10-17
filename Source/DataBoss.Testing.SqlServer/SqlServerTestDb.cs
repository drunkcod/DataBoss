using System;
using System.Collections.Concurrent;
using System.Data.SqlClient;
using System.Linq;
using DataBoss.Data;

namespace DataBoss.Testing.SqlServer
{
	public class SqlServerTestDb : IDisposable
	{
		const string CreateInstanceQuery = 
			  "create database [{0}];\n"
			+ "alter database [{0}] set recovery simple;\n"
			+ "declare @now datetime = getdate();\n"
			+ "exec [{0}]..sp_addextendedproperty @name='testdb_created_at', @value=@now;\n";

		const string DropInstanceQuery = 
			  "alter database [{0}] set single_user with rollback immediate;\n"
			+ "drop database [{0}];\n";

		static readonly ConcurrentDictionary<string, SqlServerTestDb> DatabaseInstances = new ConcurrentDictionary<string, SqlServerTestDb>();

		readonly TestDbConfig config;

		public string ConnectionString => config.ToString();
		public string Name => config.Name;

		SqlServerTestDb(TestDbConfig config) {
			this.config = config;
		}

		public void Dispose() => 
			ForceDropDatabase(config);

		public static SqlServerTestDb Create(TestDbConfig config = null) => GetTemporaryInstance(config);

		public static SqlServerTestDb GetOrCreate(string name) => GetOrCreate(new TestDbConfig { Name = name });

		public static SqlServerTestDb GetOrCreate(TestDbConfig config) {
			if(config?.Name == null)
				throw new ArgumentNullException("config.Name");
			return GetTemporaryInstance(config);
		}

		static string RandomName() => Guid.NewGuid().ToString();

		static SqlServerTestDb GetTemporaryInstance(TestDbConfig config) {
			config = FinalizeConfig(config);
			return DatabaseInstances.GetOrAdd(config.Name, key => {
				ExecuteServerCommands(config, cmd => {
					var found = (int)cmd.ExecuteScalar("select case when exists(select null from sys.databases where name = @name) then 1 else 0 end", new { name = key });
					cmd.Parameters.Clear();
					if(found == 1)
						ForceDropDatabase(config);
					cmd.ExecuteNonQuery(string.Format(CreateInstanceQuery, key));
				});
				return new SqlServerTestDb(config);
			});
		}

		static TestDbConfig FinalizeConfig(TestDbConfig config) {
			if (config == null)
				return new TestDbConfig { Name = RandomName() };
			return new TestDbConfig {
				Name = config.Name ?? RandomName(),
				Server = config.Server,
				Username = config.Username,
				Password = config.Password,
			};
		}

		public static void DeleteInstances() {
			foreach(var item in DatabaseInstances.Values.ToArray()) 
				item.Dispose();
		}

		static void ForceDropDatabase(TestDbConfig config) {
			DatabaseInstances.TryRemove(config.Name, out _);
			ExecuteServerCommands(config, cmd => cmd.ExecuteNonQuery(string.Format(DropInstanceQuery, config.Name)));
		}

		public static void RegisterForAutoCleanup() =>
			AppDomain.CurrentDomain.DomainUnload += delegate { DeleteInstances(); };

		static void ExecuteServerCommands(TestDbConfig config, Action<SqlCommand> execute) {
			var cmd = new SqlCommand { 
				Connection = config.GetServerConnection(),
			};
			try {
				cmd.Connection.Open();
				execute(cmd);
			} finally {
				cmd.Connection.Dispose();
				cmd.Dispose();
			}
		}
	}
}
