using Xunit;

namespace DataBoss.Data.SqlClient
{
	public class DataBossBulkCopySpec : IClassFixture<SqlServerFixture>
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
