using System;
using System.Diagnostics;
using System.IO;

namespace DataBoss
{
	class DataBossLogMigrationScope : IDataBossMigrationScope
	{
		readonly IDataBossLog log;
		readonly IDataBossMigrationScope inner;
		readonly Stopwatch stopwatch;

		public DataBossLogMigrationScope(IDataBossLog log, IDataBossMigrationScope inner) {
			this.log = log;
			this.inner = inner;
			this.stopwatch = new Stopwatch();

			inner.OnError += (_, e) => log.Error(e.GetException());
		}

		public event EventHandler<ErrorEventArgs> OnError {
			add { inner.OnError += value; }
			remove { inner.OnError -= value; }
		}

		public void Begin(DataBossMigrationInfo info) {
			log.Info("  Applying '{0}') {1}", info.FullId, info.Name);
			stopwatch.Restart();
			inner.Begin(info);
		}

		public bool Execute(DataBossQueryBatch query) {
			return inner.Execute(query); 
		}

		public void Done() {
			log.Info("  Finished in {0}", stopwatch.Elapsed);
			inner.Done();
		}

		void IDisposable.Dispose() {
			inner.Dispose();
		}
	}
}