using System;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace DataBoss.Data
{
	public class ProfiledSqlConnection : DbConnection
	{
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

		public event EventHandler<ProfiledSqlCommandExecutingEventArgs> CommandExecuting;
		public event EventHandler<ProfiledSqlCommandExecutedEventArgs> CommandExecuted;

		protected override DbCommand CreateDbCommand() => new ProfiledSqlCommand(this, inner.CreateCommand());
		protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel) => inner.BeginTransaction(isolationLevel);
		public override void Close() => inner.Close();
		public override void ChangeDatabase(string databaseName) => inner.ChangeDatabase(databaseName);
		public override void Open() => inner.Open();
		public override Task OpenAsync(CancellationToken cancellationToken) => inner.OpenAsync(cancellationToken);

		internal T Execute<T>(ProfiledSqlCommand command, Func<ProfiledSqlCommand, T> @do) {
			OnExecuting(command);
			var t = Stopwatch.StartNew();
			var r = @do(command);
			t.Stop();
			OnExecuted(command, t.Elapsed);
			return r;
		}

		void OnExecuting(ProfiledSqlCommand command) => CommandExecuting?.Invoke(this, new ProfiledSqlCommandExecutingEventArgs(command));
		void OnExecuted(ProfiledSqlCommand command, TimeSpan elapsed) => CommandExecuted?.Invoke(this, new ProfiledSqlCommandExecutedEventArgs(command, elapsed));
	}

	public class ProfiledSqlCommand : DbCommand
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

		public new SqlParameterCollection Parameters => inner.Parameters;

		protected override DbConnection DbConnection {
			get => parent;
			set => throw new NotSupportedException("Can't switch connection for profiled command");
		}

		protected override DbParameterCollection DbParameterCollection => inner.Parameters;

		protected override DbTransaction DbTransaction {
			get => inner.Transaction;
			set => inner.Transaction = (SqlTransaction)value;
		}

		public override void Cancel() => inner.Cancel();

		public override int ExecuteNonQuery() => parent.Execute(this, DoExecuteNonQuery);
		public override object ExecuteScalar() => parent.Execute(this, DoExecuteScalar);
		protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior) =>
			parent.Execute(this, x => x.inner.ExecuteReader(behavior));

		public override void Prepare() => inner.Prepare();

		protected override DbParameter CreateDbParameter() => inner.CreateParameter();

	}

	public class ProfiledSqlCommandExecutingEventArgs : EventArgs
	{
		public readonly ProfiledSqlCommand Command;

		public ProfiledSqlCommandExecutingEventArgs(ProfiledSqlCommand command) {
			this.Command = command;
		}
	}

	public class ProfiledSqlCommandExecutedEventArgs : EventArgs
	{
		public readonly ProfiledSqlCommand Command;
		public readonly TimeSpan Elapsed;

		public ProfiledSqlCommandExecutedEventArgs(ProfiledSqlCommand command, TimeSpan elapsed) {
			this.Command = command;
			this.Elapsed = elapsed;
		}
	}
}
