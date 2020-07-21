using System;
using System.Data.SqlClient;
using Cone;
using DataBoss.Data;
using DataBoss.Testing.SqlServer;
using Xunit;

namespace DataBoss.Testing
{
	public class DatabaseAutoCleanupRegistration
	{
		public DatabaseAutoCleanupRegistration() =>
			SqlServerTestDb.RegisterForAutoCleanup();
	}

	public class SqlServerTestDbSpec : IClassFixture<DatabaseAutoCleanupRegistration>
	{
		[Fact]
		public void creates_instance_on_first_request() {
			var name = Guid.NewGuid().ToString();

			Check.That(() => CountDatabasesByName(name) == 0);
			SqlServerTestDb.GetOrCreate(name);
			Check.That(() => CountDatabasesByName(name) == 1);			
		}

		[Fact]
		public void instance_is_created_only_once() {
			var name = Guid.NewGuid().ToString();
			Check.That(() => SqlServerTestDb.GetOrCreate(name) == SqlServerTestDb.GetOrCreate(name));
		}

		[Fact]
		public void is_deleted_when_disposed()
		{
			var db = SqlServerTestDb.Create();
			Check.That(() => CountDatabasesByName(db.Name) == 1);
			db.Dispose();
			Check.That(() => CountDatabasesByName(db.Name) == 0);
		}

		int CountDatabasesByName(string name) => (int)ExecuteScalar(
			"select count(*) from master.sys.databases where name = @name", new {
				name
		});

		object ExecuteScalar<T>(string query, T args) {
			using(var db = new SqlConnection("Server=.;Integrated Security=SSPI")) {
				db.Open();
				return db.ExecuteScalar(query, args);
			}
		}
	}
}
