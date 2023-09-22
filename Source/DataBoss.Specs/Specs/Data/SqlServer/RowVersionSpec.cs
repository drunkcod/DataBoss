using System.Data.SqlClient;
using CheckThat;
using Org.BouncyCastle.Asn1;
using Xunit;

namespace DataBoss.Data.SqlServer
{
	[Trait("Category", "Database")]
	public class RowVersionSpec : IClassFixture<SqlServerFixture>
	{
		readonly SqlServerFixture db;
		public RowVersionSpec(SqlServerFixture db) {
			this.db = db;
		}
		[Fact]
		public void Info_from_SequenceDataReader() {
			using(var c = new SqlConnection(db.ConnectionString))
			{
				c.Open();
				c.Into("#Temp", SequenceDataReader.Items(new { Version = RowVersion.From(1L) }));
				Check.That(() => (int)c.ExecuteScalar("select count(*) from #Temp") == 1);
			}
		}
	}
}
