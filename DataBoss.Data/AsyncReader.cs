using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;

namespace DataBoss.Data
{
	public static class AsyncReader
	{
		public static AsyncReader<T> StartNew<T>(Func<IDataReader> getReader, int? maxBacklog = null) =>
			new AsyncReader<T>(NewQueue<T>(maxBacklog), getReader);

		public static AsyncReader<T> StartNew<T>(IEnumerable<T> items, int? maxBacklog = null) =>
			new AsyncReader<T>(NewQueue<T>(maxBacklog), items);

		static BlockingCollection<T> NewQueue<T>(int? maxBacklog) =>
			maxBacklog.HasValue ? new BlockingCollection<T>(maxBacklog.Value) : new BlockingCollection<T>();
	}

	public class AsyncReader<T>
	{
		readonly BlockingCollection<T> items;
		public Task Task { get; }
		public Exception Exception { get; private set; }

		internal AsyncReader(BlockingCollection<T> items, IEnumerable<T> source) {
			this.items = items;
			this.Task = Task.Factory.StartNew(ReadItems, source, TaskCreationOptions.LongRunning);
		}

		internal AsyncReader(BlockingCollection<T> items, Func<IDataReader> getReader) {
			this.items = items;
			this.Task = Task.Factory.StartNew(ReadRows, getReader, TaskCreationOptions.LongRunning);
		}

		public IEnumerable<T> GetConsumingEnumerable() {
			do {
				if (items.TryTake(out var item, Timeout.Infinite))
					yield return item;
			} while (!items.IsCompleted);
			if (Exception != null)
				ExceptionDispatchInfo.Capture(Exception).Throw();
		}

		void ReadItems(object obj) {
			var source = (IEnumerable<T>)obj;
			IEnumerator<T> xs = null;
			try {
				xs = source.GetEnumerator();
				while (xs.MoveNext())
					items.Add(xs.Current);
			}
			catch (Exception ex) {
				Exception = ex;
			}
			finally {
				items.CompleteAdding();
				xs?.Dispose();
			}
		}

		void ReadRows(object obj) {
			var getReader = (Func<IDataReader>)obj;
			IDisposable disposableReader = null;
			try {
				var reader = ObjectReader.For(getReader());
				disposableReader = reader;
				reader.Read<T>(items.Add);
			}
			catch (Exception ex) {
				Exception = ex;
			}
			finally {
				items.CompleteAdding();
				disposableReader?.Dispose();
			}
		}
	}
}
