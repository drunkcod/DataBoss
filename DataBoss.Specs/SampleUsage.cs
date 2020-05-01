using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Cone;
using DataBoss.Data;

namespace DataBoss.Specs
{
	[Feature("Sample Usage")]
	public class SampleUsage
	{
		public void simple_query()
		{
			using(IDbConnection db = new SqlConnection("Server=.;Integrated Security=SSPI"))
			{
				db.Open();
				Check.That(() => (int)db.ExecuteScalar("select value = @value", new { value = 42 }) == 42);
			}

		}
	}
}
