using System.Data.SqlClient;
using System.Linq;
using Cone;
using DataBoss.Data;

namespace DataBoss.Specs.Data
{
	[Describe(typeof(DbObjectReader), Category = "Database")]
	public class DbObjectReaderSpec
	{
		struct Row { public int Value; }

		public void multi_resultset_query() {
			var db = new SqlConnection("Server=.;Integrated Security=SSPI");
			db.Open();
			var reader = new DbObjectReader(db);

			Check.With(() => reader
				.Query("select Value = @Value", new[] { 1, 2 }, x => new { Value = x })
				.Read<Row>().ToList())
			.That(
				rows => rows.Count == 2,
				rows => rows[0].Value == 1,
				rows => rows[1].Value == 2);
		}

		public void empty_input_gives_empty_output() {
			var db = new SqlConnection("Server=.;Integrated Security=SSPI");
			db.Open();
			var reader = new DbObjectReader(db);

			Check.With(() => reader
				.Query("select Value = @Value", new int[0], x => new { Value = x })
				.Read<Row>().ToList())
			.That(rows => rows.Count == 0);
		}
	}
}
