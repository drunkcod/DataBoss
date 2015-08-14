using System;
using System.IO;

namespace DataBoss
{
	public interface IDataBossMigrationScope : IDisposable
	{
		event EventHandler<ErrorEventArgs> OnError;

		bool IsFaulted { get; }

		void Begin(DataBossMigrationInfo info);
		void Execute(string query);
		void Done();
	}
}