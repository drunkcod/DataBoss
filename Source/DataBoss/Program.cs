using DataBoss.Linq;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;

namespace DataBoss
{
	delegate int DataBossAction(DataBoss program);

	public class Program
	{
		const string ProgramName = "dotnet databoss";

		static int Main(string[] args) {
			if(args.Length == 0) {
				Console.WriteLine(GetUsageString());
				return 0;
			}

			var log = new DataBossConsoleLog();
			try {
				var cc = ParseCommandConfig(args);

				if (!TryGetCommand(cc.Key, out DataBossAction command)) {
					log.Info(GetUsageString());
					return -1;
				}

				using var db = DataBoss.Create(cc.Value, log);
				return command(db);

			} catch(Exception e) {
				log.Error(e);
				log.Info(GetUsageString());
				return -1;
			}
		}

		public static KeyValuePair<string, DataBossConfiguration> ParseCommandConfig(IEnumerable<string> args) => ParseCommandConfig(args, DataBossConfiguration.Load);

		public static KeyValuePair<string, DataBossConfiguration> ParseCommandConfig(IEnumerable<string> args, Func<string, DataBossConfiguration> load) {
			var parsedArgs = PowerArgs.Parse(args);
			var config = GetBaseConfig(parsedArgs, load);
			var command = parsedArgs.Commands.SingleOrDefault() ?? throw new ArgumentException("missing command and/or configuration options.");

			parsedArgs.Into(config);

			return new KeyValuePair<string, DataBossConfiguration>(command, config);
		}

		private static DataBossConfiguration GetBaseConfig(PowerArgs parsedArgs, Func<string, DataBossConfiguration> load) {
			if (parsedArgs.TryGetArg("Target", out var target))
				return load(target);

			var targets = Directory.GetFiles(".", "*.databoss");
			if (targets.Length != 1)
				throw new ArgumentException("Can't autodetect target, use -Target <file> to specify it");
			return load(targets[0]);
		}

		static bool TryGetCommand(string name, out DataBossAction command) {
			var target = typeof(DataBoss)
				.GetMethods(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic)
				.Select(x => new {
					Method = x,
					Command = x.SingleOrDefault<DataBossCommandAttribute>()
				})
				.FirstOrDefault(x => x.Command?.Name == name);
			if(target == null)
				command = null;
			else
				command = (DataBossAction)Delegate.CreateDelegate(typeof(DataBossAction), target.Method);
			return command != null;
		}

		static string GetUsageString() =>
			ReadResource("Usage")
			.Replace("{{ProgramName}}", ProgramName)
			.Replace("{{Version}}", typeof(Program).Assembly.GetName().Version.ToString());

		static string ReadResource(string path) {
			using var reader = new StreamReader(typeof(Program).Assembly.GetManifestResourceStream(path));
			return reader.ReadToEnd();
		}
	}
}
