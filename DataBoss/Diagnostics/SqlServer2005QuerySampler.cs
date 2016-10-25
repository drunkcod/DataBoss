using System.Collections.Generic;
using DataBoss.Data;

namespace DataBoss.Diagnostics
{
	public class SqlServer2005QuerySampler : ISqlServerQuerySampler
	{
		readonly DbObjectReader reader;

		public SqlServer2005QuerySampler(DbObjectReader reader) {
			this.reader = reader;
		}

		public IEnumerable<QuerySample> TakeSample(QuerySampleMode mode) {
			var foo = mode == QuerySampleMode.ActiveDatabase ? "and r.database_id = db_id()" : string.Empty;
			return reader.Read<QuerySample>($@"
				select
					[request.SessionId] = r.session_id,
					[request.RequestId] = r.request_id,
					[request.StartTime] = r.start_time,
					[request.ElapsedMilliseconds] = datediff(ms, r.start_time, getdate()),
					[request.StatementStartOffset] = r.statement_start_offset,
					[request.StatementEndOffset] = r.statement_end_offset,
					[request.PercentComplete] = r.percent_complete,
					[request.HostName] = s.host_name,
					[request.LoginName] = s.login_name,
					[request.ProgramName] = s.program_name,
					sql = cast(text as varbinary(max))
				from sys.dm_exec_requests r
				inner join sys.dm_exec_sessions s on s.session_id = r.session_id
				cross apply sys.dm_exec_sql_text(r.sql_handle)
				where r.session_id != @@spid " + foo);
		}
	}
}