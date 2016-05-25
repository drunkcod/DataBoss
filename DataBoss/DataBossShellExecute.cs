using System.Collections.Generic;
using System.Diagnostics;

namespace DataBoss
{
	public class DataBossShellExecute
	{
		public event DataReceivedEventHandler OutputDataReceived;

		public bool Execute(string command, IEnumerable<KeyValuePair<string, string>> environmentVariables)
		{
			var argPos = command.IndexOf(' ');
			var args = string.Empty;
			if(argPos != -1) {
				args = command.Substring(argPos);
				command = command.Substring(0, argPos);
			}

			var si = new ProcessStartInfo
			{
				UseShellExecute = false,
				FileName = command,
				Arguments = args,
				RedirectStandardOutput = true,
			};
			foreach(var item in environmentVariables)
				si.EnvironmentVariables.Add(item.Key, item.Value);

			var p = Process.Start(si);
			p.EnableRaisingEvents = true;
			p.OutputDataReceived += (_ , e) => OutputDataReceived?.Invoke(this, e);
			p.BeginOutputReadLine();
			p.WaitForExit();
			return p.ExitCode == 0;
		}
	}
}