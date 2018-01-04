using System;
using System.Collections.Generic;
using System.Linq;
using Cone;
using DataBoss.Data.Dataflow;

namespace DataBoss.Specs.Dataflow
{
	[Feature("Dataflow")]
    public class DataflowFeature
    {
		public void faulted_generator_raises_error_on_completion() =>
			Check.Exception<Exception>(() => Block.Generator(FailWith(new Exception()), NullSink).Completion.Wait());

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

		static Func<IEnumerable<int>> FailWith(Exception ex) => () => { throw ex; };

		IMessageSink<int> NullSink(IActionBlock block) => null;
    }
}
