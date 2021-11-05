using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Text;

namespace DataBoss
{
	public class DataBossShellExecute
	{
		public event DataReceivedEventHandler OutputDataReceived;

		readonly Encoding OutputEncoding;

		public DataBossShellExecute(Encoding outputEncoding = null) {
			this.OutputEncoding = outputEncoding ?? Encoding.Default;
		}

		public bool Execute(string workingDir, string command, params (string Key, string Value)[] environmentVariables) {
			var argPos = command.IndexOf(' ');
			var args = string.Empty;
			if(argPos != -1) {
				args = command.Substring(argPos);
				command = command.Substring(0, argPos);
			}

			var si = new ProcessStartInfo {
				UseShellExecute = false,
				FileName = Path.Combine(workingDir, command),
				Arguments = args,
				RedirectStandardOutput = true,
				StandardOutputEncoding = OutputEncoding,
				WorkingDirectory = workingDir,
			};

			foreach(var (key, value) in environmentVariables)
				si.EnvironmentVariables.Add(key, value);

			var p = Process.Start(si);
			p.EnableRaisingEvents = true;
		
			p.OutputDataReceived += (_ , e) => OutputDataReceived?.Invoke(this, e);
			p.BeginOutputReadLine();
			p.WaitForExit();
			return p.ExitCode == 0;
		}
	}
}