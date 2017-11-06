using System.Collections.Generic;
using System.Linq;
using DataBoss.Data;

namespace DataBoss.Diagnostics
{
	public class SqlServer2000QuerySampler : ISqlServerQuerySampler
	{
		#pragma warning disable CS0649
		class RequestInfo2000
		{
			public RequestInfo Request;
			public byte[] sql_handle;
		}

		struct QueryText { public byte[] text; }
		#pragma warning restore CS0649

		readonly DbObjectReader reader;
	
		public SqlServer2000QuerySampler(DbObjectReader reader) {
			this.reader = reader;
		}

		public IEnumerable<QuerySample> TakeSample(QuerySampleMode mode) {
			var foo = mode == QuerySampleMode.ActiveDatabase ? "and r.database_id = db_id()" : string.Empty;
			var reqs = reader.Read<RequestInfo2000>(@"
				select
					[Request.SessionId] = r.session_id,
					[Request.RequestId] = r.request_id,
					[Request.StartTime] = r.start_time,
					[Request.ElapsedMilliseconds] = datediff(ms, r.start_time, getdate()),
					[Request.StatementStartOffset] = r.statement_start_offset,
					[Request.StatementEndOffset] = r.statement_end_offset,
					[Request.PercentComplete] = r.percent_complete,
					[Request.HostName] = s.host_name,
					[Request.LoginName] = s.login_name,
					[Request.ProgramName] = s.program_name,
					sql_handle
				from sys.dm_exec_requests r
				join sys.dm_exec_sessions s on s.session_id = r.session_id
				where sql_handle is not null
				and r.session_id != @@spid " + foo)
				.ToList();

			return reader.Query(@"
				select text = convert(varbinary(max), convert(nvarchar(max), text)) 
				from sys.fn_get_sql(@sql_handle)",
				reqs, x => new { x.sql_handle })
				.Read<QueryText>()
				.Zip(reqs, (s,r) => new QuerySample(r.Request, s.text));
		}
	}
}