using System;
using System.Collections.Generic;
using System.Linq;
using Cone;

namespace DataBoss.Specs
{
	[Describe(typeof(SetDifference<,>))]
	public class SequenceComparerSpec
	{
		public void find_missing_elements() {
			var missing = new List<int>();
			var differences = new SetDifference<int,int> { 
				OnMissing = missing.Add,
			};
			var a = new [] { 1, 2, 3};
			var b = new [] { 2, 3, 4};

			differences.Symmetric(a, id => id, b, id => id);
			Check.That(
				() => missing.Count == 1, 
				() => missing[0] == a.Except(b).Single());
		}

		public void find_extra_elements() {
			var extra = new List<int>();
			var differenes = new SetDifference<int, int> {
				OnExtra= extra.Add,
			};
			var a = new[] { 1, 3, 4 };
			var b = new[] { 2, 3, 4 };

			differenes.Symmetric(a, id => id, b, id => id);
			Check.That(
				() => extra.Count == 1,
				() => extra[0] == b.Except(a).Single());
		}

		public void trailing_extras() {
			var extra = new List<int>();
			var differences = new SetDifference<int, int> {
				OnExtra = extra.Add,
			};
			var a = new[] { 1, 2 };
			var b = new[] { 1, 2, 3, 4 };

			differences.Symmetric(a, id => id, b, id => id);
			Check.With(() => b.Except(a))
			.That(
				expected => expected.Count() == extra.Count,
				expected => expected.SequenceEqual(extra));
		}

		public void trailing_missing() {
			var missing = new List<int>();
			var differences = new SetDifference<int, int> {
				OnMissing = missing.Add,
			};
			var a = new[] { 1, 2, 3, 4};
			var b = new[] { 1, 2, };

			differences.Symmetric(a, id => id, b, id => id);
			Check.With(() => a.Except(b))
			.That(
				expected => expected.Count() == missing.Count,
				expected => expected.SequenceEqual(missing));
		}

		public void sequence_key_types() {
			var knownIds = new [] { 1, 3 };
			var items = new[] { Item(1, "A"), Item(2, "B"), Item(3, "C") };

			var missing = new List<KeyValuePair<int, string>>();
			var differenes = new SetDifference<KeyValuePair<int, string>, int> {
				OnMissing = missing.Add,
			};

			differenes.Symmetric(items, x => x.Key, knownIds, x => x);
			Check.That(() => missing.Count == 1, () => missing[0].Key == 2);
		}

		public void assumes_sorted_inputs() {
			var differences = new SetDifference<int, int>();
			var a = new[] { 2, 1, };
			var b = new[] { 1, 2, };

			Check.Exception<InvalidOperationException>(() => differences.Symmetric(a, id => id, b, id => id));
			Check.Exception<InvalidOperationException>(() => differences.Symmetric(b, id => id, a, id => id));
		}

		KeyValuePair<TKey, TValue> Item<TKey, TValue>(TKey key, TValue value) => new KeyValuePair<TKey, TValue>(key, value);
	}
}
