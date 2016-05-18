using System;
using System.Text;

namespace DataBoss.Diagnostics
{
	public class QuerySample
	{
		public QuerySample(RequestInfo request, byte[] sql) {
			this.Request = request;
			this.Query = Encoding.Unicode.GetString(sql);
			this.ActiveStatement = Encoding.Unicode.GetString(sql, 
				request.StatementStartOffset, 
				(request.StatementEndOffset == -1 ? sql.Length : request.StatementEndOffset) - request.StatementStartOffset);
		}
		public readonly RequestInfo Request;
		public TimeSpan Elapsed => TimeSpan.FromMilliseconds(Request.ElapsedMilliseconds);
		public readonly string ActiveStatement;
		public readonly string Query;
	}
}