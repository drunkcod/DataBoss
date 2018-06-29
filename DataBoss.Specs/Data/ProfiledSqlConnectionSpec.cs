using System;
using System.Data.Common;
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
			var expectedCommand = "select 42";
			var commandExecuting = new EventSpy<ProfiledSqlCommandExecutingEventArgs>(
				(s, e) => Check.That(() => e.Command.CommandText == expectedCommand));
			con.CommandExecuting += commandExecuting;
			
			var q = con.CreateCommand();
			q.CommandText = expectedCommand;

			Check.That(
				() => (int)q.ExecuteScalar() == 42,
				() => commandExecuting.HasBeenCalled);
		}

		public void listens_to_ExecuteNonQuery() {
			var expectedCommand = "select Id = 2 into #Foo union all select 3";
			var commandExecuting = new EventSpy<ProfiledSqlCommandExecutingEventArgs>(
				(s, e) => Check.That(() => e.Command.CommandText == expectedCommand));
			con.CommandExecuting += commandExecuting;

			var q = con.CreateCommand();
			q.CommandText = expectedCommand;

			Check.That(
				() => q.ExecuteNonQuery() == 2,
				() => commandExecuting.HasBeenCalled);
		}

		public void listens_to_ExecuteReader() {
			var expectedCommand = "select Id = 2 union all select 3";
			var commandExecuting = new EventSpy<ProfiledSqlCommandExecutingEventArgs>(
				(s, e) => Check.That(() => e.Command.CommandText == expectedCommand));
			con.CommandExecuting += commandExecuting;

			var q = con.CreateCommand();
			q.CommandText = expectedCommand;

			Check.With(() => ObjectReader.For(q.ExecuteReader()).Read<IdRow<int>>().ToList())
				.That(r => r.Count == 2, _ => commandExecuting.HasBeenCalled);
		}

		public void counts_rows_read() {
			var q = con.CreateCommand();
			q.CommandText = "select Id = 2 union all select 3 union all select 1";
			var commandExecuted = new EventSpy<ProfiledSqlCommandExecutedEventArgs>(
				(s, e) => Check.That(() => e.RowCount == 3));
			con.CommandExecuted += commandExecuted;
			using(var r = ObjectReader.For(q.ExecuteReader()))
				r.Read<IdRow<int>>().ToList();
			Check.That(() => commandExecuted.HasBeenCalled);

		}

		public void CommandExecuted_after_CommandExecuting() {
			var commandExecuting = new EventSpy<ProfiledSqlCommandExecutingEventArgs>();
			con.CommandExecuting += commandExecuting;

			var commandExecuted = new EventSpy<ProfiledSqlCommandExecutedEventArgs>((_, e) => 
				Check.That(() => e.Elapsed > TimeSpan.Zero));
			con.CommandExecuted += commandExecuted;

			var q = con.CreateCommand();
			q.CommandText = "select 1";
			q.ExecuteScalar();

			Check.That(() => commandExecuting.CalledBefore(commandExecuted));
		}
	}
}
