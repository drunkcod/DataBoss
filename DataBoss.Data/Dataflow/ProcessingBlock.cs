using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace DataBoss.Data.Dataflow
{
	public interface IActionBlock
	{
		Task Completion { get; }
	}

	public interface IMessageSink<T>
	{
		void Post(T item);
	}

	public class MessageChannel<T> : IMessageSink<T>
	{
		readonly List<Action<T>> targets = new List<Action<T>>();

		public void Post(T item) => targets.ForEach(x => x(item));

		public MessageChannel<T> ConnectTo<TTarget>(IMessageSink<TTarget> target, Func<T, TTarget> transform) =>
			ConnectTo(x => target.Post(transform(x)));

		public MessageChannel<T> ConnectTo(Action<T> action) {
			targets.Add(action);
			return this;
		}
	}

	public interface IProcessingBlock<T> : IActionBlock, IMessageSink<T>
	{
		void Complete();
	}

	public static class Block
	{
		class TaskBlock : IActionBlock		
		{
			readonly Task worker;

			internal TaskBlock(Task worker) {
				this.worker = worker;
			}

			public Task Completion => worker;
		}

		class SequenceProcessingBlock<T> : IProcessingBlock<T>
		{
			readonly BlockingCollection<T> inputs;
			readonly Action<IEnumerable<T>> process;
			readonly Task worker;

			internal SequenceProcessingBlock(TaskFactory tasks, Action<IEnumerable<T>> process, int? maxQueue) {
				this.inputs = maxQueue.HasValue ? new BlockingCollection<T>(maxQueue.Value) : new BlockingCollection<T>();
				this.process = process;
				this.worker = tasks.StartNew(() => process(inputs.GetConsumingEnumerable()), TaskCreationOptions.LongRunning);
			}

			public Task Completion => worker;
			public void Complete() => inputs.CompleteAdding();

			public void Post(T item) => inputs.Add(item);
		}

		public static IActionBlock Action(Action action) => 
			new TaskBlock(Task.Factory.StartNew(action, TaskCreationOptions.LongRunning));

		public static IActionBlock Generator<T>(Func<IEnumerable<T>> generator, Func<IActionBlock, IMessageSink<T>> getSink) {
			var completion = new TaskCompletionSource<object>();
			var block = new TaskBlock(completion.Task);
			var sink = getSink(block);
			Task.Factory.StartNew(() => {
				try {
					foreach (var item in generator())
						sink.Post(item);
					completion.SetResult(null);
				}
				catch (Exception ex) {
					completion.SetException(ex);
				}
			}, TaskCreationOptions.LongRunning);
			return block;
		}

		public static IProcessingBlock<T> Sequence<T>(Action<IEnumerable<T>> process, int? maxQueue = null) => 
			new SequenceProcessingBlock<T>(Task.Factory, process, maxQueue);

		public static IProcessingBlock<IEnumerable<T>> Batches<T>(Action<IEnumerable<T>> process, int? maxQueue = null) => 
			Sequence<IEnumerable<T>>(xs => process(xs.SelectMany(x => x)), maxQueue);
	}
}
