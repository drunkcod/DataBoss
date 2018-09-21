using System.Collections.Generic;
using Cone;
using DataBoss.Collections;

namespace DataBoss.Specs
{
	[Describe(typeof(BlockCollection<>))]
	public class BlockCollectionSpec
	{
		public void implements_IReadonlyCollection() => 
			Check.That(() => typeof(IReadOnlyCollection<int>).IsAssignableFrom(typeof(BlockCollection<int>)));
	}
}