using System.Collections.Generic;
using System.Linq;
using CheckThat;
using Xunit;

namespace DataBoss.Linq
{
	public class Enumerators_
	{
		[Fact]
		public void ToArray() => Check.That(
			() => GetEnumerator(Items(1)).ToArray().SequenceEqual(Items(1)),
			() => GetEnumerator(Items(117)).ToArray().SequenceEqual(Items(117)));

		IEnumerable<int> Items(int count) => Enumerable.Range(0, count).Select(x => x);

		IEnumerator<T> GetEnumerator<T>(IEnumerable<T> items) => items.GetEnumerator();
	}
}
