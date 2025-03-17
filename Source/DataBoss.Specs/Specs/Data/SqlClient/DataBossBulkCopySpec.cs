using Microsoft.Data.SqlClient;
using Xunit;

namespace DataBoss.Data.SqlClient
{
	public class DataBossBulkCopySpec(SqlServerFixture db) : IClassFixture<SqlServerFixture>
	{
		readonly SqlServerFixture db = db;

		SqlConnection GetConnection() {
			var c = new SqlConnection(db.ConnectionString);
			c.Open();
			return c;
		}

		[Fact]
		public void target_that_requires_quoted_name() {
			using var c = GetConnection();
			c.Into("#need-to-be-quoted", SequenceDataReader.Items(new { Id = 1 }, new { Id = 2 }));
		}
	}
}
