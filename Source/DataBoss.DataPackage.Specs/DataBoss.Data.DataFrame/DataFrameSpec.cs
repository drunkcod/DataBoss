using System.Collections.Generic;
using System.Linq;
using CheckThat;
using Xunit;

namespace DataBoss.Data.DataFrames
{
	public class DataFrameSpec
	{
		[Fact]
		public void create_from_DataReader() {
			var items = SequenceDataReader.Items(
				new { Id = 1, Message = "Hello", Value = (float?)42 },
				new { Id = 2, Message = "World", Value = default(float?) });
			var df = DataFrame.Create(items);

			Check.That(
				() => df.Columns.Count == items.FieldCount,
				() => df["Id"] is IEnumerable<int>,
				() => df["Message"] is IEnumerable<string>,
				() => df["Value"] is IEnumerable<float?>,
				() => df.Count == 2);
		}

		[Fact]
		public void column_gt() {
			var items = SequenceDataReader.Create(Enumerable
				.Range(0, 3)
				.Select(x => new { N = x }));
			var df = DataFrame.Create(items);
			var filter = df.GetColumn<int>("N") > 1;
			Check.That(
				() => filter.Count == 3,
				() => filter.SequenceEqual(new[] { false, false, true }));

		}

		[Fact]
		public void filter_column() {
			var items = SequenceDataReader.Create(Enumerable
				.Range(0, 10)
				.Select(x => new { N = x }));
			var df = DataFrame.Create(items);
			var filtered = df[df.GetColumn<int>("N") > 5];
			Check.That(() => filtered.Count == 4);
		}
	}
}
