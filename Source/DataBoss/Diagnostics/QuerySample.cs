using System;
using System.Text;

namespace DataBoss.Diagnostics
{
	public class QuerySample
	{
		public QuerySample(RequestInfo request, byte[] sql) {
			this.Request = request;
			this.Query = Encoding.Unicode.GetString(sql);
		}
		public readonly RequestInfo Request;
		public TimeSpan Elapsed => TimeSpan.FromMilliseconds(Request.ElapsedMilliseconds);
		public readonly string Query;
		public string ActiveStatement => Request.StatementEndOffset == -1 
			? Query.Substring(Request.StatementStartOffset / 2)
			: Query.Substring(Request.StatementStartOffset / 2, (Request.StatementEndOffset - Request.StatementStartOffset) / 2 + 1);
	}
}