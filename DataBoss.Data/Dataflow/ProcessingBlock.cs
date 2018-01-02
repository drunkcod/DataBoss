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

		public static IActionBlock Generator<T>(Func<IEnumerable<T>> generator, Func<IMessageSink<T>> getSink) {
			var sink = getSink();
			return new TaskBlock(Task.Factory.StartNew(() => {

				foreach (var item in generator())
				sink.Post(item);
			}, TaskCreationOptions.LongRunning));
		}

		public static IProcessingBlock<T> Sequence<T>(Action<IEnumerable<T>> process, int? maxQueue = null) => 
			new SequenceProcessingBlock<T>(Task.Factory, process, maxQueue);

		public static IProcessingBlock<IEnumerable<T>> Batches<T>(Action<IEnumerable<T>> process, int? maxQueue = null) => 
			Sequence<IEnumerable<T>>(xs => process(xs.SelectMany(x => x)), maxQueue);
	}
}
