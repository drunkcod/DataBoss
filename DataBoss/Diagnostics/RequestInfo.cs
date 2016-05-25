using System;

namespace DataBoss.Diagnostics
{
	public class RequestInfo
	{
		public short SessionId;
		public string HostName;
		public string LoginName;
		public string ProgramName;
		public int RequestId;
		public DateTime StartTime;
		public int ElapsedMilliseconds;
		public int StatementStartOffset;
		public int StatementEndOffset;
		public float PercentComplete;
	}
}
