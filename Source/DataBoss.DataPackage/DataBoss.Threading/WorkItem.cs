using System;
using System.Threading;
using System.Threading.Tasks;

namespace DataBoss.Threading
{
	abstract class WorkItem
	{
		Thread thread;

		protected abstract void DoWork();
		protected virtual void Cleanup() { }

		public Task RunAsync() {
			if (thread != null && thread.IsAlive)
				throw new InvalidOperationException("WorkItem already started.");
			thread = new Thread(Run) {
				IsBackground = true,
				Name = GetType().Name,
			};
			var tsc = new TaskCompletionSource<int>();
			thread.Start(tsc);

			return tsc.Task;
		}

		void Run(object obj) {
			var tsc = (TaskCompletionSource<int>)obj;
			try {
				DoWork();
				tsc.SetResult(0);
			}
			catch (OperationCanceledException) {
				tsc.SetCanceled();
			} catch (Exception ex) {
				tsc.SetException(new Exception("CSV writing failed.", ex));
			}
			finally {
				Cleanup();
			}
		}

		public void Join() => thread.Join();
	}

}
