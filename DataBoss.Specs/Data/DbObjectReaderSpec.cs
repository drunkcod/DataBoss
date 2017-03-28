using Cone;
using DataBoss.Data;
using System.Data.SqlClient;
using System.Linq;

namespace DataBoss.Specs.Data
{
	[Describe(typeof(DbObjectReader), Category = "Database")]
	public class DbObjectReaderSpec
	{
		DbObjectReader DbReader;

		struct Row { public int Value; }

		[BeforeEach]
		public void given_a_object_reader() {
			var db = new SqlConnection("Server=.;Integrated Security=SSPI");
			db.Open();
			DbReader = new DbObjectReader(db);
		}

		public void multi_resultset_query() {
			Check.With(() => DbReader
				.Query("select Value = @Value", new[] { 1, 2 }, x => new { Value = x })
				.Read<Row>().ToList())
			.That(
				rows => rows.Count == 2,
				rows => rows[0].Value == 1,
				rows => rows[1].Value == 2);
		}

		public void empty_input_gives_empty_output() {
			Check.With(() => DbReader
				.Query("select Value = @Value", new int[0], x => new { Value = x })
				.Read<Row>().ToList())
			.That(rows => rows.Count == 0);
		}
	}
}
