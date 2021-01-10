using System.Collections.Generic;
using CheckThat;
using Xunit;

namespace DataBoss.Collections
{
	public class BlockCollectionSpec
	{
		[Fact]
		public void implements_IReadonlyCollection() => 
			Check.That(() => typeof(IReadOnlyCollection<int>).IsAssignableFrom(typeof(BlockCollection<int>)));
	}
}