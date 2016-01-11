using System;

namespace DataBoss
{
	public interface IDataBossLog
	{
		void Info(string format, params object[] args);
		void Error(Exception exception);
	}

	public class NullDataBossLog : IDataBossLog
	{
		public void Info(string format, params object[] args) { }
		public void Error(Exception exception) { }
	}
}