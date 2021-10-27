namespace DataBoss.Testing.SqlServer
{
	public class SqlServerTestDbSessionInfo
	{
		public readonly short SessionId;
		public readonly string HostName;
		public readonly string ApplicationName;
		public readonly string EventType;
		public readonly string EventInfo;

		public SqlServerTestDbSessionInfo(short session_id, string host_name, string program_name, string event_type, string event_info) {
			this.SessionId = session_id;
			this.HostName = host_name;
			this.ApplicationName = program_name;
			this.EventType = event_type;
			this.EventInfo = event_info;
		}
	}

}
