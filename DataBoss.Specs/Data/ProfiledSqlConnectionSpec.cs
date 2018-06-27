using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Cone;
using Cone.Helpers;
using DataBoss.Data;
using DataBoss.Data.Common;

namespace DataBoss.Specs.Data
{
	public class ProfiledSqlConnection : DbConnection
	{
		class ProfiledSqlCommand : DbCommand
		{
			static readonly Func<ProfiledSqlCommand, int> DoExecuteNonQuery = x => x.inner.ExecuteNonQuery();
			static readonly Func<ProfiledSqlCommand, object> DoExecuteScalar = x => x.inner.ExecuteScalar();

			readonly ProfiledSqlConnection parent;
			readonly SqlCommand inner;

			public ProfiledSqlCommand(ProfiledSqlConnection parent, SqlCommand inner) {
				this.parent = parent;
				this.inner = inner;
			}

			public override string CommandText {
				get => inner.CommandText;
				set => inner.CommandText = value;
			}

			public override int CommandTimeout { 
				get => inner.CommandTimeout; 
				set => inner.CommandTimeout = value; 
			}

			public override CommandType CommandType { 
				get => inner.CommandType; 
				set => inner.CommandType = value;
			}

			public override bool DesignTimeVisible { 
				get => inner.DesignTimeVisible; 
				set => inner.DesignTimeVisible = value; 
			}

			public override UpdateRowSource UpdatedRowSource { 
				get => inner.UpdatedRowSource; 
				set => inner.UpdatedRowSource = value; 
			}

			protected override DbConnection DbConnection { 
				get => parent; 
				set => throw new NotSupportedException("Can't switch connection for profiled command");
			}

			protected override DbParameterCollection DbParameterCollection => inner.Parameters;

			protected override DbTransaction DbTransaction { 
				get => inner.Transaction;
				set => throw new NotSupportedException("Can't assign transaction to profiled command");
			}

			public override void Cancel() => inner.Cancel();

			public override int ExecuteNonQuery() => parent.Execute(this, DoExecuteNonQuery);
			public override object ExecuteScalar() => parent.Execute(this, DoExecuteScalar);
			protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior) => 
				parent.Execute(this, x => x.inner.ExecuteReader(behavior));

			public override void Prepare() => inner.Prepare();

			protected override DbParameter CreateDbParameter() => inner.CreateParameter();

		}

		readonly SqlConnection inner;

		public ProfiledSqlConnection(SqlConnection inner) {
			this.inner = inner;
		}

		public override string ConnectionString { 
			get => inner.ConnectionString; 
			set => inner.ConnectionString = value; 
		}
	
		public override string Database => inner.Database;
		public override string DataSource => inner.DataSource;
		public override string ServerVersion => inner.ServerVersion;
		public override ConnectionState State => inner.State;

		public event EventHandler<ProfiledCommandExecutingEventArgs> CommandExecuting;
		public event EventHandler<ProfiledCommandExecutedEventArgs> CommandExecuted;

		protected override DbCommand CreateDbCommand() => new ProfiledSqlCommand(this, inner.CreateCommand());
		protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel) => inner.BeginTransaction(isolationLevel);
		public override void Close() => inner.Close();
		public override void ChangeDatabase(string databaseName) => inner.ChangeDatabase(databaseName);
		public override void Open() => inner.Open();
		public override Task OpenAsync(CancellationToken cancellationToken) => inner.OpenAsync(cancellationToken);

		T Execute<T>(ProfiledSqlCommand command, Func<ProfiledSqlCommand, T> @do) {
			OnExecuting(command);
			var t = Stopwatch.StartNew();
			var r = @do(command);
			t.Stop();
			OnExecuted(command, t.Elapsed);
			return r;
		}

		void OnExecuting(ProfiledSqlCommand command) => CommandExecuting?.Invoke(this, new ProfiledCommandExecutingEventArgs(command));
		void OnExecuted(ProfiledSqlCommand command, TimeSpan elapsed) => CommandExecuted?.Invoke(this, new ProfiledCommandExecutedEventArgs(command, elapsed));
	}

	public class ProfiledCommandExecutingEventArgs : EventArgs 
	{ 
		public readonly DbCommand Command;

		public ProfiledCommandExecutingEventArgs(DbCommand command) {
			this.Command = command;
		}
	}

	public class ProfiledCommandExecutedEventArgs : EventArgs
	{
		public readonly DbCommand Command;
		public readonly TimeSpan Elapsed;

		public ProfiledCommandExecutedEventArgs(DbCommand command, TimeSpan elapsed) {
			this.Command = command;
			this.Elapsed = elapsed;
		}
	}

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
			var commandExecuting = new EventSpy<ProfiledCommandExecutingEventArgs>(
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
			var commandExecuting = new EventSpy<ProfiledCommandExecutingEventArgs>(
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
			var commandExecuting = new EventSpy<ProfiledCommandExecutingEventArgs>(
				(s, e) => Check.That(() => e.Command.CommandText == expectedCommand));
			con.CommandExecuting += commandExecuting;

			var q = con.CreateCommand();
			q.CommandText = expectedCommand;

			Check.With(() => ObjectReader.For(q.ExecuteReader()).Read<IdRow<int>>().ToList())
				.That(r => r.Count == 2, _ => commandExecuting.HasBeenCalled);
		}

		public void CommandExecuted_after_CommandExecuting() {
			var commandExecuting = new EventSpy<ProfiledCommandExecutingEventArgs>();
			con.CommandExecuting += commandExecuting;

			var commandExecuted = new EventSpy<ProfiledCommandExecutedEventArgs>((_, e) => 
				Check.That(() => e.Elapsed > TimeSpan.Zero));
			con.CommandExecuted += commandExecuted;

			var q = con.CreateCommand();
			q.CommandText = "select 1";
			q.ExecuteScalar();

			Check.That(() => commandExecuting.CalledBefore(commandExecuted));
		}
	}
}
