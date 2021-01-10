using System.Data.SqlClient;
using CheckThat;
using Xunit;

namespace DataBoss.Data.SqlServer
{
	[Trait("Category", "Database")]
	public class RowVersionSpec
	{
		[Fact]
		public void Info_from_SequenceDataReader() {
			using(var db = new SqlConnection("Server=.;Integrated Security=SSPI"))
			{
				db.Open();
				db.Into("#Temp", SequenceDataReader.Items(new { Version = RowVersion.From(1L) }));
				Check.That(() => (int)db.ExecuteScalar("select count(*) from #Temp") == 1);
			}
		}
	}
}
