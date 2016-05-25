using System;
using System.IO;

namespace DataBoss
{
	public interface IDataBossMigrationScope : IDisposable
	{
		event EventHandler<ErrorEventArgs> OnError;

		void Begin(DataBossMigrationInfo info);
		bool Execute(DataBossQueryBatch query);
		void Done();
	}
}