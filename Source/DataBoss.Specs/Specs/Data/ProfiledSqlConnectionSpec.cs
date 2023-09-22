using System;
using System.Data.SqlClient;
using System.Linq;
using CheckThat;
using CheckThat.Helpers;
using DataBoss.Data.Common;
using DataBoss.Linq;
using Xunit;

namespace DataBoss.Data
{
	public sealed class ProfiledSqlConnectionSpec : IClassFixture<SqlServerFixture>, IDisposable
    {
		readonly ProfiledSqlConnection con;

		public ProfiledSqlConnectionSpec(SqlServerFixture db) { 
			con = new ProfiledSqlConnection(new SqlConnection(db.ConnectionString));
			con.Open();
		}

		void IDisposable.Dispose() => con.Dispose();

		[Fact]
		public void listens_to_ExecuteScalar() {
			var commandExecuting = new EventSpy<ProfiledSqlCommandExecutingEventArgs>();
			con.CommandExecuting += commandExecuting;

			var expectedCommand = "select 42";
			Check.That(() => (int)con.ExecuteScalar(expectedCommand) == 42);
			commandExecuting.Check((_, e) => e.Command.CommandText == expectedCommand);;
		}

		[Fact]
		public void listens_to_ExecuteNonQuery() {
			var commandExecuting = new EventSpy<ProfiledSqlCommandExecutingEventArgs>(				);
			con.CommandExecuting += commandExecuting;

			var expectedCommand = "select Id = 2 into #Foo union all select 3";
			Check.That(() => con.ExecuteNonQuery(expectedCommand) == 2);
			commandExecuting.Check((_, e) => e.Command.CommandText == expectedCommand);
		}

		[Fact]
		public void listens_to_ExecuteReader() {
			var commandExecuting = new EventSpy<ProfiledSqlCommandExecutingEventArgs>();
			con.CommandExecuting += commandExecuting;

			var expectedCommand = "select Id = 2 union all select 3";
			var q = con.CreateCommand(expectedCommand);

			Check.With(() => ObjectReader.For(() => q.ExecuteReader()).Read<IdRow<int>>().ToList())
				.That(r => r.Count == 2);
			commandExecuting.Check((_, e) => e.Command.CommandText == expectedCommand);
		}

		[Fact]
		public void counts_rows_read() {
			var readerClosed = new EventSpy<ProfiledDataReaderClosedEventArgs>();
			var commandExecuted = new EventSpy<ProfiledSqlCommandExecutedEventArgs>((_, e) => 
			{
				e.DataReader.Closed += readerClosed;
			});
			con.CommandExecuted += commandExecuted;

			var q = con.CreateCommand("select Id = 2 union all select 3 union all select 1");
			var r = ObjectReader.For(() => q.ExecuteReader());
				r.Read<IdRow<int>>().ToList();
			readerClosed.Check((_, e) => e.RowCount == 3);
			commandExecuted.Check((_, e) => e.RowCount == 0);
		}

		[Fact]
		public void CommandExecuted_after_CommandExecuting() {
			var commandExecuting = new EventSpy<ProfiledSqlCommandExecutingEventArgs>();
			con.CommandExecuting += commandExecuting;

			var commandExecuted = new EventSpy<ProfiledSqlCommandExecutedEventArgs>();
			con.CommandExecuted += commandExecuted;

			con.ExecuteScalar("select 1");
			commandExecuted.Check((_, e) => e.Elapsed > TimeSpan.Zero);
			Check.That(() => commandExecuting.CalledBefore(commandExecuted));
		}
	}
}
