using System.Collections.Generic;
using Cone;
using DataBoss.Collections;
using Xunit;

namespace DataBoss.Specs
{
	public class BlockCollectionSpec
	{
		[Fact]
		public void implements_IReadonlyCollection() => 
			Check.That(() => typeof(IReadOnlyCollection<int>).IsAssignableFrom(typeof(BlockCollection<int>)));
	}
}