using System;
using System.Data.SqlClient;
using Cone;
using DataBoss.Data;
using Xunit;

namespace DataBoss.Testing
{
	public class DatabaseAutoCleanupRegistration
	{
		public DatabaseAutoCleanupRegistration() =>
			DatabaseSetup.RegisterForAutoCleanup();
	}

	public class DatabaseSetupSpec : IClassFixture<DatabaseAutoCleanupRegistration>
	{
		[Fact]
		public void creates_instance_on_first_request() {
			var name = Guid.NewGuid().ToString();

			Check.That(() => CountDatabasesByName(name) == 0);

			DatabaseSetup.GetTemporaryInstance(name);

			Check.That(() => CountDatabasesByName(name) == 1);			
		}

		[Fact]
		public void instance_is_created_only_once() {
			var name = Guid.NewGuid().ToString();
			Check.That(() => DatabaseSetup.GetTemporaryInstance(name) == DatabaseSetup.GetTemporaryInstance(name));
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
