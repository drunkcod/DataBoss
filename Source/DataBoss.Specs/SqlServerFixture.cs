using System;
using System.Data.SqlClient;
using DataBoss.Testing.SqlServer;

namespace DataBoss
{
	public sealed class SqlServerFixture : IDisposable
	{
		readonly SqlServerTestDb testDb;
		public SqlConnection Connection { get; private set; }
		public string ConnectionString => testDb.ConnectionString;

		public SqlServerFixture() {
			this.testDb = SqlServerTestDb.Create();
			Connection = new SqlConnection(testDb.ConnectionString);
			Connection.Open();
		}

		void IDisposable.Dispose() {
			Connection.Dispose();
			testDb.Dispose();
		}
	}
}
