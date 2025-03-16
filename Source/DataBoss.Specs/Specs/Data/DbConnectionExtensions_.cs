using CheckThat;
using Microsoft.Data.SqlClient;
using Xunit;

namespace DataBoss.Data
{
	public class DbConnectionExtensions_
	{
		[Fact]
		public void dont_double_wrap_IDbConnection_to_IDataBossConnection() {
			var dataBossConnection = new SqlConnection().WithCommandTimeout(0);

			Check.That(() => DbConnectionExtensions.Wrap(dataBossConnection) == dataBossConnection);
		}
	}
}
