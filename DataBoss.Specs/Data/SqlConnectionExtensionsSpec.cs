using Cone;
using DataBoss.Data;

namespace DataBoss.Specs.Data
{
	[Describe(typeof(SqlConnectionExtensions), Category = "Database")]
    public class SqlConnectionExtensionsSpec
    {
		DataBossConnectionProvider connections = new DataBossConnectionProvider("Server=.;Integrated Security=SSPI");
		
		[AfterEach]
		public void cleanup() => connections.Cleanup();

		public void ExecuteNonQuery_flows_local_transaction() {
			var db = connections.OpenConnection();
			using(var t = db.BeginTransaction())
				db.ExecuteNonQuery(t, "select 42");
		}
    }
}
