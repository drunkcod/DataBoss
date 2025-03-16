using System;
using DataBoss.Testing.SqlServer;
using Microsoft.Data.SqlClient;

namespace DataBoss.Data
{
	public sealed class TemporaryDatabaseFixture : IDisposable
	{
		readonly SqlServerTestDb testDb;
		public string ConnectionString => testDb.ConnectionString;

		public TemporaryDatabaseFixture() {
			this.testDb = SqlServerTestDb.Create();
		}

		public void Dispose() =>
			testDb.Dispose();

		public SqlConnection Open() {
			var c = new SqlConnection(ConnectionString);
			c.Open();
			return c;
		}
	}
}
