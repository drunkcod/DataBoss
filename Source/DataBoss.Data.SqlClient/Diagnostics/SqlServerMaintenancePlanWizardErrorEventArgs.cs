#if MSSQLCLIENT
using DataBoss.Data.MsSql;
using Microsoft.Data.SqlClient;
#else
	using System.Data.SqlClient;
#endif

using System;

namespace DataBoss.Diagnostics
{
	public class SqlServerMaintenancePlanWizardErrorEventArgs : EventArgs
	{
		public readonly SqlConnection Connection;
		public readonly Exception Error;

		public SqlServerMaintenancePlanWizardErrorEventArgs(SqlConnection connection, Exception error) {
			this.Connection = connection;
			this.Error = error;
		}
	}
}
