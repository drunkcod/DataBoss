using System.Linq;
using Cone;
using DataBoss.Data.Common;
using Xunit;

namespace DataBoss.Data
{
	public class MultiDataReaderSpec
	{
		[Fact]
		public void reads_elements_in_round_robin_fashion() {
			var reader = new MultiDataReader(
				SequenceDataReader.Create(new[]{ 1, 3, 5 }.Select(x => new IdRow<int> { Id = x })),
				SequenceDataReader.Create(new[]{ 2, 4 }.Select(x => new IdRow<int> { Id = x })));

			Check.With(() => ObjectReader.For(reader).Read<IdRow<int>>().ToList()).That(
				xs => xs.Count == 5,
				xs => xs[0].Id == 1,
				xs => xs[1].Id == 2);
		}

		[Fact]
		public void handle_jagged_readers() {
			var reader = new MultiDataReader(
				SequenceDataReader.Create(new[] { 1, 4 }.Select(x => new IdRow<int> { Id = x })),
				SequenceDataReader.Create(new[] { 2 }.Select(x => new IdRow<int> { Id = x })),
				SequenceDataReader.Create(new[] { 3, 5, 6 }.Select(x => new IdRow<int> { Id = x })));

			Check.With(() => ObjectReader.For(reader).Read<IdRow<int>>().ToList()).That(
				xs => xs.Count == 6,
				xs => xs[0].Id == 1,
				xs => xs[5].Id == 6);
		}
	}
}
 