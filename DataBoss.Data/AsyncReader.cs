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
		public static AsyncReader<T> StartNew<T>(IEnumerable<T> items, int? maxBacklog = null) =>
			AsyncReader<T>.StartNew(items, maxBacklog);
	}

	public class AsyncReader<T>
	{
		delegate void ItemProducer(object state, ref IDisposable cleanup);

		readonly BlockingCollection<T> items;
		public Task Task { get; private set; }
		public Exception Exception { get; private set; }

		internal AsyncReader(BlockingCollection<T> items) {
			this.items = items;
		}

		public static AsyncReader<T> StartNew(IEnumerable<T> items, int? maxBacklog = null) {
			var reader = new AsyncReader<T>(NewQueue(maxBacklog));
			reader.StartProducer(reader.ProduceItems, items);
			return reader;
		}

		public static AsyncReader<T> StartNew<TReader>(Func<TReader> getReader, int? maxBacklog = null) where TReader : IDataReader {
			var reader = new AsyncReader<T>(NewQueue(maxBacklog));
			reader.StartProducer(reader.ProduceRows<TReader>, getReader);
			return reader;
		}

		static BlockingCollection<T> NewQueue(int? maxBacklog) =>
			maxBacklog.HasValue ? new BlockingCollection<T>(maxBacklog.Value) : new BlockingCollection<T>();

		void StartProducer(ItemProducer producer, object state) {
			this.Task = Task.Factory.StartNew(RunProducer, Tuple.Create(producer, state), TaskCreationOptions.LongRunning);
		}

		public IEnumerable<T> GetConsumingEnumerable() {
			do {
				if (items.TryTake(out var item, Timeout.Infinite))
					yield return item;
			} while (!items.IsCompleted);
			if (Exception != null)
				ExceptionDispatchInfo.Capture(Exception).Throw();
		}

		void RunProducer(object obj)
		{
			var (producer, state) = (Tuple<ItemProducer, object>)obj;
			IDisposable cleanup = null;
			try {
				producer(state, ref cleanup);
			}
			catch (Exception ex) {
				Exception = ex;
			}
			finally {
				items.CompleteAdding();
				cleanup?.Dispose();
			}
		}

		void ProduceItems(object obj, ref IDisposable cleanup) {
			var source = (IEnumerable<T>)obj;
			var xs = source.GetEnumerator();
			cleanup = xs;
			while(xs.MoveNext())
				items.Add(xs.Current);
		}

		void ProduceRows<TReader>(object obj, ref IDisposable cleanup) where TReader : IDataReader {
			var getReader = (Func<TReader>)obj;
			var reader = ObjectReader.For(getReader());
			cleanup = reader;
			reader.Read<T>(items.Add);
		}
	}
}
