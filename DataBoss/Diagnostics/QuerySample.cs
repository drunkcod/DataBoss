using System;
using System.Text;

namespace DataBoss.Diagnostics
{
	public class QuerySample
	{
		public QuerySample(RequestInfo request, byte[] sql) {
			this.Request = request;
			this.Query = Encoding.Unicode.GetString(sql);

			var count = (request.StatementEndOffset == -1 ? sql.Length : request.StatementEndOffset) - request.StatementStartOffset; 
			var start = request.StatementStartOffset;
			this.ActiveStatement = Encoding.Unicode.GetString(sql, 
				Math.Min(sql.Length - 1, start), 
				Math.Max(0, count));
		}
		public readonly RequestInfo Request;
		public TimeSpan Elapsed => TimeSpan.FromMilliseconds(Request.ElapsedMilliseconds);
		public readonly string ActiveStatement;
		public readonly string Query;
	}
}