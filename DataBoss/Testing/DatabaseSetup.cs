using System;
using System.Collections.Concurrent;
using System.Data.SqlClient;

namespace DataBoss.Testing
{
	public static class DatabaseSetup
	{
		public static Func<string,string> FormatInstanceName = x => $"💣 {x}";
		public static string ServerConnectionString = "Server=.;Integrated Security=SSPI";

		static readonly ConcurrentDictionary<string, SqlConnectionStringBuilder> DatabaseInstances = new ConcurrentDictionary<string, SqlConnectionStringBuilder>();

		public static SqlConnectionStringBuilder GetTemporaryInstance(string name) => GetTemporaryInstance(name, _ => { });

		public static SqlConnectionStringBuilder GetTemporaryInstance(string name, Action<SqlConnectionStringBuilder> init) =>
			DatabaseInstances.GetOrAdd(name, key => {
				var instanceName = FormatInstanceName(key);
				ExecuteCommands(cmd => {
					var found = (int)cmd.ExecuteScalar("select case when exists(select null from sys.databases where name = @instanceName) then 1 else 0 end", new { instanceName });
					if(found == 1)
						ForceDropDatabase(instanceName);
					cmd.ExecuteNonQuery($"create database [{instanceName}]");
					cmd.ExecuteNonQuery($"alter database[{instanceName}] set recovery simple");
				});
				var cs = new SqlConnectionStringBuilder(ServerConnectionString) { InitialCatalog = instanceName };
				init(cs);
				return cs;
			});

		public static void DeleteInstances() {
			SqlConnectionStringBuilder ignored;
			foreach(var item in DatabaseInstances.ToArray()) {
				ForceDropDatabase(FormatInstanceName(item.Key));
				DatabaseInstances.TryRemove(item.Key, out ignored);
			}
		}

		static void ForceDropDatabase(string instanceName) {
			ExecuteCommands(cmd => {
				cmd.ExecuteNonQuery($"alter database [{instanceName}] set single_user with rollback immediate");
				cmd.ExecuteNonQuery($"drop database [{instanceName}]");
			});
		}

		public static void RegisterForAutoCleanup() =>
			AppDomain.CurrentDomain.DomainUnload += (_, __) => DeleteInstances();

		static void ExecuteCommands(Action<SqlCommand> execute) {
			using(var db = new SqlConnection(ServerConnectionString)) {
				db.Open();
				db.WithCommand(execute);
			}
		}
	}
}
