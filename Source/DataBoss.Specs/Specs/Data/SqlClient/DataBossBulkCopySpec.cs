using System;
using System.Data.SqlClient;
using DataBoss.Testing.SqlServer;
using Xunit;

namespace DataBoss.Data.SqlClient
{
	public class SqlServerFixture : IDisposable
	{
		public SqlConnection Connection { get; private set; }

		public SqlServerFixture() {
			Connection = new SqlConnection(SqlServerTestDb.GetOrCreate(nameof(SqlServerFixture)).ConnectionString);
			Connection.Open();
		}

		void IDisposable.Dispose() {
			Connection.Dispose();
			Connection = null;
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
