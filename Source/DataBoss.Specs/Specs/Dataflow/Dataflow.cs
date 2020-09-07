using System;
using System.Collections.Generic;
using CheckThat;
using DataBoss.Data.Dataflow;
using Xunit;

namespace DataBoss.Specs.Dataflow
{
	public class DataflowFeature
    {
		[Fact]
		public void faulted_generator_raises_error_on_completion() =>
			Check.Exception<Exception>(() => Block.Generator(FailWith(new Exception()), NullSink).Completion.Wait());

		[Fact]
		public void broadcast_channel() {
			var postedItem = 42;
		
			var channel = new MessageChannel<int>();
			var results = new List<string>();

			channel.ConnectTo(x => results.Add($"Action:{x}"));

			var matchingSink = new ActionSink<int>(x => results.Add($"T:{x}"));
			channel.ConnectTo(matchingSink);

			var transformSink = new ActionSink<string>(x => results.Add("Transform:" + x));
			channel.ConnectTo(transformSink, x => x.ToString());

			channel.Post(postedItem);
			Check.That(
				() => results.Contains($"Action:{postedItem}"),
				() => results.Contains($"Action:{postedItem}"),
				() => results.Contains($"Action:{postedItem}"));
		}

		[Fact]
		public void faulted_consumer_back_propagates_error() {
			var problem  = new Exception();
			var consumer = Block.Item((int _) => throw problem, 1);
			
			consumer.Post(1);
			consumer.Completion.ContinueWith(_ => { }).Wait();
			Check.That(() => consumer.Completion.IsFaulted);

			var e = Check.Exception<InvalidOperationException>(() => consumer.Post(2));
			Check.That(() => e.InnerException == problem);
		}

		[Fact]
		public void faulting_while_stuck_on_post_is_propagated() {
			var problem = new Exception();
			var consumer = Block.Item((int _) => { }, 1);

			consumer.Post(1);
			var producer = Block.Generator(AllTheNumbers, _ => consumer);
			consumer.Complete();
			producer.Completion.ContinueWith(_ => { }).Wait();

			Check.That(() => producer.Completion.Exception.InnerException is InvalidOperationException);
		}

		static IEnumerable<int> AllTheNumbers() { 
			for(var n = 0;; ++n)
				yield return n;
		}

		static Func<IEnumerable<int>> FailWith(Exception ex) => () => { throw ex; };

		IMessageSink<int> NullSink(IActionBlock block) => null;
    }
}
