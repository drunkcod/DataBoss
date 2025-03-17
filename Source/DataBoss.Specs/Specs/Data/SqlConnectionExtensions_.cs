using System.Linq;
using CheckThat;
using DataBoss.Diagnostics;
using Microsoft.Data.SqlClient;
using Xunit;

namespace DataBoss.Data.MsSql
{
	public sealed class SqlConnectionExtensions_(SqlServerFixture db) : IClassFixture<SqlServerFixture>
	{
		readonly SqlServerFixture db = db;

		SqlConnection GetConnection() {
			var c = new SqlConnection(db.ConnectionString);
			c.Open();
			return c;
		}

#pragma warning disable CS0649
		class MyStuffRow
		{
			public int Id;
			public string Value;
		}
#pragma warning restore CS0649

		[Fact]
		public void insert_and_retreive_ids_is_zippable() {
			using var c = GetConnection();
			var destinationTableName = "#MyStuff";
			c.ExecuteNonQuery($"create table {destinationTableName}(Id int identity, Value varchar(max) not null,)");

			var newItems = new[]{
				new { Value = "First" },
				new { Value = "Second" },
				new { Value = "Third" },
			};
			var newIds = c.InsertAndGetIdentities(destinationTableName, newItems);
			var myStuff = SqlObjectReader.Create(c).Read<MyStuffRow>($"select * from {destinationTableName}");

			Check.With(() => newIds.Zip(myStuff, (x, item) => new { SequenceId = x, item.Id, item.Value }).ToList())
				.That(
					zipped => zipped[0] == new { SequenceId = 1, Id = 1, Value = "First" },
					zipped => zipped[1] == new { SequenceId = 2, Id = 2, Value = "Second" },
					zipped => zipped[2] == new { SequenceId = 3, Id = 3, Value = "Third" });
		}

		[Fact]
		public void db_info() {
			using var c = GetConnection();
			Check
				.With(() => c.GetDatabaseInfo())
				.That(db => db.DatabaseName == (string)c.ExecuteScalar("select db_name()"));
		}

		[Fact]
		public void ExecuteNonQuery_flows_local_transaction() {
			using var c = GetConnection();

			using var tx = c.BeginTransaction();
			c.ExecuteNonQuery(tx, "select 42");
		}

		[Fact]
		public void into_supports_transactions() {
			using var c = GetConnection();

			using var tx = c.BeginTransaction();
			c.Into(tx, "#TempRows", SequenceDataReader.Items(new { Id = 1 }));
			c.Into(tx, "#TempRows2", new[] { new { Id = 2 } });
		}
	}
}
