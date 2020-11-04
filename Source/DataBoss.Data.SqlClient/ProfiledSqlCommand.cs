#if MSSQLCLIENT
namespace DataBoss.Data.MsSql
{
	using Microsoft.Data.SqlClient;
#else
namespace DataBoss.Data
{
	using System.Data.SqlClient;
#endif

	using System.Data;
	using System.Data.Common;

	public class ProfiledSqlCommand : DbCommand
	{
		readonly internal SqlCommand inner;

		internal ProfiledSqlCommand(ProfiledSqlConnection parent, SqlCommand inner) {
			this.Connection = parent;
			this.inner = inner;
		}

		public ProfiledSqlCommand(SqlCommand inner) : this(null, inner) { }

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

		public new ProfiledSqlConnection Connection { get; set; }

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
			get => Connection;
			set {
				Connection = (ProfiledSqlConnection)value;
				inner.Connection = (SqlConnection)Connection;
			}
		}

		protected override DbParameterCollection DbParameterCollection => inner.Parameters;

		protected override DbTransaction DbTransaction {
			get => inner.Transaction;
			set => inner.Transaction = (SqlTransaction)value;
		}

		public override void Cancel() => inner.Cancel();

		public override int ExecuteNonQuery() {
			var s = Connection.OnExecuting(this);
			var r = inner.ExecuteNonQuery();
			s.OnExecuted(r, null);
			return r;
		}

		public override object ExecuteScalar() {
			var s = Connection.OnExecuting(this);
			var r = inner.ExecuteScalar();
			s.OnExecuted(1, null);
			return r;
		}

		public new ProfiledDataReader ExecuteReader(CommandBehavior behavior) => new ProfiledDataReader(inner.ExecuteReader(behavior), inner.Connection.GetScripter());

		protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior) {
			var s = Connection.OnExecuting(this);
			var r = ExecuteReader(behavior);
			s.OnExecuted(0, r);
			return r;
		}

		public override void Prepare() => inner.Prepare();

		protected override DbParameter CreateDbParameter() => inner.CreateParameter();
	}
}
