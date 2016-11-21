using System;
using System.Collections.Concurrent;
using System.Data.SqlClient;

namespace DataBoss.Testing
{
	public static class DatabaseSetup
	{
		public const string InstanceNameFormat = "💣 {0}";

		static readonly ConcurrentDictionary<string, string> DatabaseInstances = new ConcurrentDictionary<string, string>();

		public static string GetInstance(string name) =>
			DatabaseInstances.GetOrAdd(name, key => {
				SqlConnection.ClearAllPools();
				var instanceName = string.Format(InstanceNameFormat, key);
				WithLocalConnection(db => {
					using(var cmd = db.CreateCommand("select case when exists(select null from sys.databases where name = @instanceName) then 1 else 0 end", new { instanceName })) {
						if((int)cmd.ExecuteScalar() == 1) {
							cmd.CommandText = $"drop database [{instanceName}]";
							cmd.ExecuteNonQuery();
						}
					}
					db.ExecuteNonQuery($"create database [{instanceName}]");
					db.ExecuteNonQuery($"alter database[{instanceName}] set recovery simple");
				});
				return $"Server=.;Integrated Security=SSPI;Database={instanceName}";
			});

		public static void DeleteInstances() {
			foreach(var item in DatabaseInstances.ToArray())
				WithLocalConnection(db => {
					var instanceName = string.Format(InstanceNameFormat, item.Key);
					db.ExecuteNonQuery($"alter database [{instanceName}] set single_user with rollback immediate");
					db.ExecuteNonQuery($"drop database [{instanceName}]");
				});
			DatabaseInstances.Clear();
		}

		public static void RegisterForAutoCleanup() =>
			AppDomain.CurrentDomain.DomainUnload += (_, __) => DeleteInstances();

		static void WithLocalConnection(Action<SqlConnection> withDb) {
			using(var db = new SqlConnection("Server=.;Integrated Security=SSPI")) {
				db.Open();
				withDb(db);
			}
		}
	}
}
