using System;
using System.Diagnostics;

namespace DataBoss
{
	class DataBossConsoleLogMigrationScope : IDataBossMigrationScope
	{
		readonly IDataBossMigrationScope inner;
		readonly Stopwatch stopwatch;

		public DataBossConsoleLogMigrationScope(IDataBossMigrationScope inner) {
			this.inner = inner;
			this.stopwatch = new Stopwatch();
		}

		public void Begin(DataBossMigrationInfo info) {
			Console.WriteLine("  Applying: {0}. {1}", info.FullId, info.Name);
			stopwatch.Restart();
			inner.Begin(info);
		}

		public void Execute(string query) { inner.Execute(query); }

		public void Done() {
			Console.WriteLine("    Finished in {0}", stopwatch.Elapsed);
			inner.Done();
		}

		void IDisposable.Dispose() {
			inner.Dispose();
		}
	}
}