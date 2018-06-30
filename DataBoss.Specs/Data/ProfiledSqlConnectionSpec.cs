using System;
using System.Data.SqlClient;
using System.Linq;
using Cone;
using Cone.Helpers;
using DataBoss.Data;
using DataBoss.Data.Common;

namespace DataBoss.Specs.Data
{
	[Describe(typeof(ProfiledSqlConnection))]
    public class ProfiledSqlConnectionSpec
    {
		ProfiledSqlConnection con;

		[BeforeEach]
		public void given_a_profiled_connection() { 
			con = new ProfiledSqlConnection(new SqlConnection("Server=.;Integrated Security=SSPI"));
			con.Open();
		}

		[AfterEach]
		public void cleanup() => con.Dispose();

		public void listens_to_ExecuteScalar() {
			var commandExecuting = new EventSpy<ProfiledSqlCommandExecutingEventArgs>();
			con.CommandExecuting += commandExecuting;

			var expectedCommand = "select 42";
			Check.That(() => (int)con.ExecuteScalar(expectedCommand) == 42);
			commandExecuting.Check((_, e) => e.Command.CommandText == expectedCommand);;
		}

		public void listens_to_ExecuteNonQuery() {
			var commandExecuting = new EventSpy<ProfiledSqlCommandExecutingEventArgs>(				);
			con.CommandExecuting += commandExecuting;

			var expectedCommand = "select Id = 2 into #Foo union all select 3";
			Check.That(() => con.ExecuteNonQuery(expectedCommand) == 2);
			commandExecuting.Check((_, e) => e.Command.CommandText == expectedCommand);
		}

		public void listens_to_ExecuteReader() {
			var commandExecuting = new EventSpy<ProfiledSqlCommandExecutingEventArgs>();
			con.CommandExecuting += commandExecuting;

			var expectedCommand = "select Id = 2 union all select 3";
			var q = con.CreateCommand(expectedCommand);

			Check.With(() => ObjectReader.For(q.ExecuteReader()).Read<IdRow<int>>().ToList())
				.That(r => r.Count == 2, _ => commandExecuting.HasBeenCalled);
			commandExecuting.Check((_, e) => e.Command.CommandText == expectedCommand);
		}

		public void counts_rows_read() {
			var commandExecuted = new EventSpy<ProfiledSqlCommandExecutedEventArgs>();
			con.CommandExecuted += commandExecuted;

			var q = con.CreateCommand("select Id = 2 union all select 3 union all select 1");
			using (var r = ObjectReader.For(q.ExecuteReader()))
				r.Read<IdRow<int>>().ToList();
			commandExecuted.Check((s, e) => e.RowCount == 3);
		}

		public void CommandExecuted_after_CommandExecuting() {
			var commandExecuting = new EventSpy<ProfiledSqlCommandExecutingEventArgs>();
			con.CommandExecuting += commandExecuting;

			var commandExecuted = new EventSpy<ProfiledSqlCommandExecutedEventArgs>((_, e) => 
				Check.That(() => e.Elapsed > TimeSpan.Zero));
			con.CommandExecuted += commandExecuted;

			var q = con.CreateCommand("select 1");
			q.ExecuteScalar();

			Check.That(() => commandExecuting.CalledBefore(commandExecuted));
		}
	}
}
