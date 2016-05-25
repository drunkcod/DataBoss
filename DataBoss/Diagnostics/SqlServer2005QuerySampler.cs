using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using DataBoss.Data;

namespace DataBoss.Diagnostics
{
	public class SqlServer2005QuerySampler : ISqlServerQuerySampler
	{
		class RequestInfo2005
		{
			public RequestInfo Request;
			public byte[] text;
		}
	
		readonly DbObjectReader reader;

		public SqlServer2005QuerySampler(DbObjectReader reader) {
			this.reader = reader;
		}

		public IEnumerable<QuerySample> TakeSample(SqlConnection db)
		{
			return reader.Read<RequestInfo2005>(@"
		select
			[Request.SessionId] = r.session_id,
			[Request.RequestId] = r.request_id,
			[Request.StartTime] = r.start_time,		
			[Request.ElapsedMilliseconds] = r.total_elapsed_time,
			[Request.StatementStartOffset] = r.statement_start_offset,
			[Request.StatementEndOffset] = r.statement_end_offset,
			[Request.HostName] = s.host_name,
			[Request.LoginName] = s.login_name,
			[Request.ProgramName] = s.program_name,
			text = cast(text as varbinary(max))
        from sys.dm_exec_requests r
        cross apply sys.dm_exec_sql_text(r.sql_handle)
        inner join sys.dm_exec_sessions s on s.session_id = r.session_id
        where r.session_id != @@spid
    	").Select(x => new QuerySample(x.Request, x.text));
		}
	}
}