using System;
using System.Linq;
using Microsoft.Data.SqlClient;
using CheckThat;
using Xunit;

namespace DataBoss.Data.MsSql
{
	[Trait("Category", "Database")]
	public class DbObjectReaderSpec : IClassFixture<SqlServerFixture>, IDisposable
	{
		SqlConnection Db;
		DbObjectReader<SqlCommand, SqlDataReader> DbReader;

#pragma warning disable CS0649
		struct Row { public int Value; }
#pragma warning restore CS0649

		public DbObjectReaderSpec(SqlServerFixture db) {
			Db = new SqlConnection(db.ConnectionString);
			Db.Open();
			DbReader = SqlObjectReader.Create(Db);
		}

		void IDisposable.Dispose() {
			Db.Dispose();
		}

		[Fact]
		public void multi_resultset_query() {
			Check.With(() => DbReader
				.Query("select Value = @Value", new[] { 1, 2 }, x => new { Value = x })
				.Read<Row>().ToList())
			.That(
				rows => rows.Count == 2,
				rows => rows[0].Value == 1,
				rows => rows[1].Value == 2);
		}

		[Fact]
		public void empty_input_gives_empty_output() {
			Check.With(() => DbReader
				.Query("select Value = @Value", new int[0], x => new { Value = x })
				.Read<Row>().ToList())
			.That(rows => rows.Count == 0);
		}

		[Fact]
		public void supports_retry() {
			Db.ExecuteNonQuery("select Value = 1 into #Temp");

			var rows = DbReader.Query(@"
				insert #Temp
				select max(Value) + 1 from #Temp
				if (select count(*) from #Temp) = 2 
					raiserror('All Is Bad', 16, 1)
				select * from #Temp
			", new { }).ReadWithRetry<Row>((n, e) => n < 2);

			Check.That(
				() => rows.Count == 3,
				() => rows.Select(x => x.Value).SequenceEqual(new[] { 1, 2, 3 }));
		}

		[Fact]
		public void stops_retry_on_null_timeout() {
			Db.ExecuteNonQuery("select Value = 1 into #Temp");

			Check.Exception<SqlException>(() => DbReader.Query(@"
				insert #Temp
				select max(Value) + 1 from #Temp
				if (select count(*) from #Temp) = 2 
					raiserror('All Is Bad', 16, 1)
				select * from #Temp
			", new { }).ReadWithRetry<Row>((n, e) => false));

			Check.That(() => (int)Db.ExecuteScalar("select count(*) from #Temp") == 2);
		}

		[Fact]
		public void read_with_custom_converters() {
			var conversions = new ConverterCollection();
			conversions.Add((short x) => x + x);

			Check.With(() => DbReader.Read<Row>("select Value = cast(21 as smallint)", conversions).Single())
				.That(row => row.Value == 42);
		}

		[Fact]
		public void read_with_retry_custom_converters() {
			var conversions = new ConverterCollection();
			conversions.Add((short x) => x * 3);

			Check.With(() => DbReader
				.Query("select Value = cast(@value as smallint)", new { value = 7 })
				.ReadWithRetry<Row>((n, x) => n < 10, conversions)
				.Single()
			).That(row => row.Value == 21);
		}
	}
}
