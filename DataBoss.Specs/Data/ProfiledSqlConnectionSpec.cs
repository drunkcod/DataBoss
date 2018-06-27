using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Cone;
using Cone.Helpers;

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

			public override int CommandTimeout { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }
			public override CommandType CommandType { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }
			public override bool DesignTimeVisible { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }
			public override UpdateRowSource UpdatedRowSource { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }
			protected override DbConnection DbConnection { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }

			protected override DbParameterCollection DbParameterCollection => throw new NotImplementedException();

			protected override DbTransaction DbTransaction { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }

			public override void Cancel() {
				throw new NotImplementedException();
			}

			public override int ExecuteNonQuery() => parent.Execute(this, DoExecuteNonQuery);
			public override object ExecuteScalar() => parent.Execute(this, DoExecuteScalar);

			public override void Prepare() {
				throw new NotImplementedException();
			}

			protected override DbParameter CreateDbParameter() {
				throw new NotImplementedException();
			}

			protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior) {
				throw new NotImplementedException();
			}
		}

		readonly SqlConnection inner;

		public ProfiledSqlConnection(SqlConnection inner) {
			this.inner = inner;
		}

		public override string ConnectionString { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }

		public override string Database => throw new NotImplementedException();

		public override string DataSource => throw new NotImplementedException();

		public override string ServerVersion => throw new NotImplementedException();

		public override ConnectionState State => throw new NotImplementedException();

		public event EventHandler<ProfiledCommandExecutingEventArgs> CommandExecuting;
		public event EventHandler<ProfiledCommandExecutedEventArgs> CommandExecuted;

		protected override DbCommand CreateDbCommand() => new ProfiledSqlCommand(this, inner.CreateCommand());

		protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel) {
			throw new NotImplementedException();
		}

		public override void Close() {
			throw new NotImplementedException();
		}

		public override void ChangeDatabase(string databaseName) {
			throw new NotImplementedException();
		}

		public override void Open() => inner.Open();
		public override Task OpenAsync(CancellationToken cancellationToken) => inner.OpenAsync(cancellationToken);

		T Execute<T>(ProfiledSqlCommand command, Func<ProfiledSqlCommand, T> @do) {
			OnExecuting(command);
			var r = @do(command);
			OnExecuted(command);
			return r;
		}

		void OnExecuting(ProfiledSqlCommand command) => CommandExecuting?.Invoke(this, new ProfiledCommandExecutingEventArgs(command));
		void OnExecuted(ProfiledSqlCommand command) => CommandExecuted?.Invoke(this, new ProfiledCommandExecutedEventArgs(command));

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

		public ProfiledCommandExecutedEventArgs(DbCommand command) {
			this.Command = command;
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

		public void CommandExecuted_after_CommandExecuting() {
			var commandExecuting = new EventSpy<ProfiledCommandExecutingEventArgs>();
			con.CommandExecuting += commandExecuting;

			var commandExecuted = new EventSpy<ProfiledCommandExecutedEventArgs>();
			con.CommandExecuted += commandExecuted;

			var q = con.CreateCommand();
			q.CommandText = "select 1";
			q.ExecuteScalar();

			Check.That(() => commandExecuting.CalledBefore(commandExecuted));

		}
	}
}
