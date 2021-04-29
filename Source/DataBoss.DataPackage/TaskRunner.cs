using System;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;

namespace DataBoss.Threading
{
	class TaskRunner
	{
		readonly ManualResetEvent Completed = new(initialState: false);
		Func<Task> StartTask;
		Exception Exception;

		static void RunTask(object obj) {
			var state = (TaskRunner)obj;
			try {
				state.StartTask().Wait();
			} catch (Exception ex) {
				state.Exception = ex;
			} finally {
				state.Completed.Set();
			}
		}

		public static void Run(Func<Task> startTask) {
			var taskRunner = new TaskRunner { StartTask = startTask };
			ThreadPool.QueueUserWorkItem(RunTask, taskRunner);
			taskRunner.Completed.WaitOne();
			if (taskRunner.Exception != null)
				ExceptionDispatchInfo.Capture(taskRunner.Exception).Throw();
		}
	}
}
