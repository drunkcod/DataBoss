using System;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;

namespace DataBoss.Data
{
	public class ProfiledSqlCommand : DbCommand
	{

		static readonly ExecuteT<int> DoExecuteNonQuery = (SqlCommand c, out int result) => result = c.ExecuteNonQuery();
		static readonly ExecuteT<object> DoExecuteScalar = (SqlCommand c, out object result) => {
			result = c.ExecuteScalar();
			return 1;
		};

		readonly ProfiledSqlConnection parent;
		readonly internal SqlCommand inner;

		internal ProfiledSqlCommand(ProfiledSqlConnection parent, SqlCommand inner) {
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
		public new SqlDataReader ExecuteReader(CommandBehavior behavior) => inner.ExecuteReader(behavior);

		protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior) {
			var s = parent.OnExecuting(this);
			var reader = ExecuteReader(behavior);
			var t = parent.OnReaderCreated(this);
			var r = new ProfiledDataReader(reader, t.OnReaderClosed);
			s.OnExecuted(0);
			return r;
		}

		public override void Prepare() => inner.Prepare();

		protected override DbParameter CreateDbParameter() => inner.CreateParameter();
	}
}
