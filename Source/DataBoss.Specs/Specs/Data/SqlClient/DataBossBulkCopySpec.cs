using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using DataBoss.Data;
using DataBoss.Testing.SqlServer;
using Xunit;

namespace DataBoss.Specs.Specs.Data.SqlClient
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
