using Cone;
using DataBoss.Data;
using System;
using System.Data.SqlClient;
using System.Linq;

namespace DataBoss.Specs.Data
{
	[Describe(typeof(SqlCommandEnumerable<>), Category = "Database")]
	public class SqlCommandEnumerableSpec
	{
		SqlConnection Db;

		RetryStrategy retryAlways => (n, e) => TimeSpan.Zero;
		int rowsRead;
		int ReadInt0(SqlDataReader r) {
			++rowsRead;
			return r.GetInt32(0);
		}

		[BeforeEach]
		public void given_a_object_reader() {
			Db = new SqlConnection("Server=.;Integrated Security=SSPI");
			Db.Open();
			rowsRead = 0;
		}

		public void Single_raises_appropriate_exception_when_more_than_one_element() {
			var rows = IntRows("select * from (values(1),(2))Foo(Id)");

			Check.Exception<InvalidOperationException>(() => rows.Single(retryAlways));
		}
		
		public void Single_raises_appropriate_exception_when_no_element() {
			var rows = IntRows("select top 0 * from (values(1),(2))Foo(Id)");

			Check.Exception<InvalidOperationException>(() => rows.Single(retryAlways));
		}

		public void Single_consumes_at_most_two_elements() {
			var rows = IntRows("select * from (values(1),(2),(3))Foo(Id)");
			try { rows.Single(retryAlways); } catch { }

			Check.That(() => rowsRead <= 2);
		}

		public void SingleOrDefault_raises_appropriate_exception_when_more_than_one_element() {
			var rows = IntRows("select * from (values(1),(2))Foo(Id)");

			Check.Exception<InvalidOperationException>(() => rows.SingleOrDefault(retryAlways));
		}
		
		public void SingleOrDefault_returns_default_when_no_element() {
			var rows = IntRows("select top 0 * from (values(1),(2))Foo(Id)");

			Check.That(() => rows.SingleOrDefault(retryAlways) == default(int));
		}

		public void SingleOrDefault_consumes_at_most_two_elements() {
			var rows = IntRows("select * from (values(1),(2),(3))Foo(Id)");
			try { rows.SingleOrDefault(retryAlways); } catch { }

			Check.That(() => rowsRead <= 2);
		}

		SqlCommandEnumerable<int> IntRows(string query) =>
			new SqlCommandEnumerable<int>(() => Db.CreateCommand(query), r => ReadInt0);
	}

	[Describe(typeof(DbObjectReader), Category = "Database")]
	public class DbObjectReaderSpec
	{
		SqlConnection Db;
		DbObjectReader DbReader;

		struct Row { public int Value; }

		[BeforeEach]
		public void given_a_object_reader() {
			Db = new SqlConnection("Server=.;Integrated Security=SSPI");
			Db.Open();
			DbReader = new DbObjectReader(Db);
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

		public void supports_retry() {
			Db.ExecuteNonQuery("select Value = 1 into #Temp");

			var rows = DbReader.Query(@"
				insert #Temp
				select max(Value) + 1 from #Temp
				if (select count(*) from #Temp) = 2 
					raiserror('All Is Bad', 16, 1)
				select * from #Temp
			", new { }).ReadWithRetry<Row>((n, e) => TimeSpan.Zero);

			Check.That(
				() => rows.Count == 3,
				() => rows.Select(x => x.Value).SequenceEqual(new[] { 1, 2, 3 }));
		}

		public void stops_retry_on_null_timeout()
		{
			Db.ExecuteNonQuery("select Value = 1 into #Temp");

			Check.Exception<SqlException>(() => DbReader.Query(@"
				insert #Temp
				select max(Value) + 1 from #Temp
				if (select count(*) from #Temp) = 2 
					raiserror('All Is Bad', 16, 1)
				select * from #Temp
			", new { }).ReadWithRetry<Row>((n, e) => null));

			Check.That(() => (int)Db.ExecuteScalar("select count(*) from #Temp") == 2);
		}

	}
}
