using System;

namespace DataBoss
{
	public class DataBossConsoleLog : IDataBossLog
	{
		public void Info(string format, params object[] args)
		{
			Console.WriteLine(format, args);
		}

		public void Error(Exception exception) {
			var oldColor = Console.ForegroundColor;
			Console.ForegroundColor = ConsoleColor.Red;
			Console.Error.WriteLine(exception.Message);
			Console.Error.WriteLine(exception.StackTrace);
			Console.ForegroundColor = oldColor;
		}
	}
}