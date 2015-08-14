using System;
using System.Diagnostics;
using System.IO;

namespace DataBoss
{
	class DataBossConsoleLogMigrationScope : IDataBossMigrationScope
	{
		readonly IDataBossMigrationScope inner;
		readonly Stopwatch stopwatch;

		public DataBossConsoleLogMigrationScope(IDataBossMigrationScope inner) {
			this.inner = inner;
			this.stopwatch = new Stopwatch();

			inner.OnError += (_, e) => {
				var oldColor = Console.ForegroundColor;
				Console.ForegroundColor = ConsoleColor.DarkRed;
				Console.Error.WriteLine("  {0}", e.GetException().Message.Replace("\n" ,"\n  "));
				Console.ForegroundColor = oldColor;
			};
		}

		public event EventHandler<ErrorEventArgs> OnError {
			add { inner.OnError += value; }
			remove { inner.OnError -= value; }
		}

		public bool IsFaulted => inner.IsFaulted;

		public void Begin(DataBossMigrationInfo info) {
			Console.WriteLine("  Applying '{0}') {1}", info.FullId, info.Name);
			stopwatch.Restart();
			inner.Begin(info);
		}

		public void Execute(string query) {
				inner.Execute(query); 
		}

		public void Done() {
			Console.WriteLine("  Finished in {0}", stopwatch.Elapsed);
			inner.Done();
		}

		void IDisposable.Dispose() {
			inner.Dispose();
		}
	}
}