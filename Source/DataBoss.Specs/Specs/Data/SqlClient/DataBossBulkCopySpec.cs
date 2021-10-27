using System;
using System.Data.SqlClient;
using DataBoss.Testing.SqlServer;
using Xunit;

namespace DataBoss.Data.SqlClient
{
	public sealed class SqlServerFixture : IDisposable
	{
		readonly SqlServerTestDb testDb;
		public SqlConnection Connection { get; private set; }

		public SqlServerFixture() {
			this.testDb = SqlServerTestDb.GetOrCreate(nameof(SqlServerFixture));
			Connection = new SqlConnection(testDb.ConnectionString);
			Connection.Open();
		}

		void IDisposable.Dispose() {
			Connection.Dispose();
			testDb.Dispose();
		}
	}

	[CollectionDefinition(nameof(SqlServerFixture))]
	public class SqlServerFixtureCollection : ICollectionFixture<SqlServerFixture>
	{ }

	[Collection(nameof(SqlServerFixture))]
	public class DataBossBulkCopySpec
	{
		readonly SqlServerFixture db;
		public DataBossBulkCopySpec(SqlServerFixture db)
		{
			this.db = db;
		}

		[Fact]
		public void target_that_requires_quoted_name() {
			db.Connection.Into("#need-to-be-quoted", SequenceDataReader.Items(new { Id = 1 }, new { Id = 2 }));
		}
	}
}
