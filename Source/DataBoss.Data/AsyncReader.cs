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
		delegate void ItemProducer(AsyncReader<T> reader, object state, ref IDisposable cleanup);

		readonly BlockingCollection<T> items;
		public Task Task { get; private set; }
		public Exception Exception { get; private set; }

		internal AsyncReader(int? maxBacklog) {
			this.items = maxBacklog.HasValue ? new BlockingCollection<T>(maxBacklog.Value) : new BlockingCollection<T>();
		}

		public static AsyncReader<T> StartNew(IEnumerable<T> items, int? maxBacklog = null) =>
			StartReader(ProduceItems, items, maxBacklog);

		public static AsyncReader<T> StartNew<TReader>(Func<TReader> getReader, int? maxBacklog = null) where TReader : IDataReader =>
			StartReader(ProduceRows<TReader>, getReader, maxBacklog);

		static AsyncReader<T> StartReader(ItemProducer producer, object state, int? maxBacklog) {
			var reader = new AsyncReader<T>(maxBacklog);
			reader.Task = Task.Factory.StartNew(RunProducer, Tuple.Create(reader, producer, state), TaskCreationOptions.LongRunning);
			return reader;
		}

		public IEnumerable<T> GetConsumingEnumerable() {
			do {
				if (items.TryTake(out var item, Timeout.Infinite))
					yield return item;
			} while (!items.IsCompleted);

			if (Exception != null)
				ExceptionDispatchInfo.Capture(Exception).Throw();
		}

		static void RunProducer(object obj)
		{
			var (reader, producer, state) = (Tuple<AsyncReader<T>, ItemProducer, object>)obj;
			IDisposable cleanup = null;
			try {
				producer(reader, state, ref cleanup);
			} catch (Exception ex) {
				reader.Exception = ex;
			} finally {
				reader.items.CompleteAdding();
				cleanup?.Dispose();
			}
		}

		static void ProduceItems(AsyncReader<T> self, object obj, ref IDisposable cleanup) {
			var source = (IEnumerable<T>)obj;
			var xs = source.GetEnumerator();
			cleanup = xs;
			while(xs.MoveNext())
				self.items.Add(xs.Current);
		}

		static void ProduceRows<TReader>(AsyncReader<T> self, object obj, ref IDisposable cleanup) where TReader : IDataReader {
			var getReader = (Func<TReader>)obj;
			var reader = ObjectReader.For(getReader);
			cleanup = null;
			reader.Read<T>(self.items.Add);
		}
	}
}
