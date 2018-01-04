using System;
using System.Collections.Generic;
using Cone;
using DataBoss.Data.Dataflow;

namespace DataBoss.Specs.Dataflow
{
	[Feature("Dataflow")]
    public class DataflowFeature
    {
		public void faulted_generator_raises_error_on_completion() =>
			Check.Exception<Exception>(() => Block.Generator(FailWith(new Exception()), NullSink).Completion.Wait());

		static Func<IEnumerable<int>> FailWith(Exception ex) => () => { throw ex; };

		IMessageSink<int> NullSink(IActionBlock block) => null;
    }
}
