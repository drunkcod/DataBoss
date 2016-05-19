using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using DataBoss.Data;

namespace DataBoss.Diagnostics
{
	public class SqlServer2000QuerySampler : ISqlServerQuerySampler
	{
		class RequestInfo2000
		{
			public RequestInfo Request;
			public byte[] sql_handle;
		}
		struct QueryText
		{
			public byte[] text;
		}

		readonly DbObjectReader reader;
	
		public SqlServer2000QuerySampler(DbObjectReader reader) {
			this.reader = reader;
		}

		public IEnumerable<QuerySample> TakeSample(SqlConnection db)
		{
			return reader.Read<RequestInfo2000>(
				@"select
			[Request.SessionId] = r.session_id,
			[Request.RequestId] = r.request_id,
			[Request.StartTime] = r.start_time,		
			[Request.StatementStartOffset] = r.statement_start_offset,
			[Request.StatementEndOffset] = r.statement_end_offset,
			[Request.HostName] = s.host_name,
			[Request.LoginName] = s.login_name,
			[Request.ProgramName] = s.program_name,
			[Request.ElapsedMilliseconds] = datediff(ms, r.start_time, getdate()),
			sql_handle
		from sys.dm_exec_requests r
		join sys.dm_exec_sessions s on s.session_id = r.session_id
		where sql_handle is not null
		and r.session_id != @@spid"
				).ToList()
				.Select(x => new
				{
					x.Request,
					Text = reader
						.Query(@"select text = convert(varbinary(max), convert(nvarchar(max), text)) from sys.fn_get_sql(@sql_handle)", new { x.sql_handle })
						.Single<QueryText>().text
				})
				.Select(x => new QuerySample(x.Request, x.Text));
		}
	}
}