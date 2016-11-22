using Cone;
using System;
using System.Data.SqlClient;

namespace DataBoss.Testing
{
	[Describe(typeof(DatabaseSetup))]
	public class DatabaseSetupSpec
	{
		[BeforeAll]
		public void RegisterForAutoCleanup() => DatabaseSetup.RegisterForAutoCleanup();

		public void creates_instance_on_first_request() {
			var name = Guid.NewGuid().ToString();

			Check.That(() => CountDatabasesByName(name) == 0);

			DatabaseSetup.GetInstance(name);

			Check.That(() => CountDatabasesByName(name) == 1);			
		}

		public void instance_is_created_only_once() {
			var name = Guid.NewGuid().ToString();
			Check.That(() => DatabaseSetup.GetInstance(name) == DatabaseSetup.GetInstance(name));
		}

		int CountDatabasesByName(string name) => (int)ExecuteScalar(
			"select count(*) from master.sys.databases where name = @instanceName", new {
				instanceName = DatabaseSetup.FormatInstanceName(name)
			});

		object ExecuteScalar<T>(string query, T args) {
			using(var db = new SqlConnection("Server=.;Integrated Security=SSPI")) {
				db.Open();
				return db.ExecuteScalar(query, args);
			}
		}
	}
}
