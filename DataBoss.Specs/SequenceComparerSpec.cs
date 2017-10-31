using System;
using System.Collections.Generic;
using System.Linq;
using Cone;

namespace DataBoss.Specs
{
	public class SequenceChangeDetector<TItem, TKnown>
	{
		public Action<TItem> OnMissing;
		public Action<TKnown> OnExtra;

		public void FindChanges<TKey>(IEnumerable<TItem> a, Func<TItem, TKey> keySelectorA , IEnumerable<TKnown> b, Func<TKnown, TKey> keySelectorB) where TKey : IComparable<TKey> {
			using(var xs = a.GetEnumerator())
			using(var ys = b.GetEnumerator()) { 
				var moreA = xs.MoveNext();
				var moreB = ys.MoveNext();
				if(moreA && moreB) {
					var keyA = keySelectorA(xs.Current);
					var keyB = keySelectorB(ys.Current);
					while (moreA && moreB) {
						var c = keyA.CompareTo(keyB);
						if (c == 0) { 
							moreA = MoveNextKey(xs, keySelectorA, out keyA);
							moreB = MoveNextKey(ys, keySelectorB, out keyB);
						} else if(c < 0) {
							OnMissing?.Invoke(xs.Current);
							moreA = MoveNextKey(xs, keySelectorA, out keyA);
						} else {
							OnExtra?.Invoke(ys.Current);
							moreB = MoveNextKey(ys, keySelectorB, out keyB);
						}
					}
				}
				if (moreA && OnMissing != null)
					do { OnMissing(xs.Current); } while ((MoveNextKey(xs, keySelectorA, out var _)));

				if (moreB && OnExtra != null)
					do { OnExtra(ys.Current); } while((MoveNextKey(ys, keySelectorB, out var _)));
			}
		}

		static bool MoveNextKey<T, TKey>(IEnumerator<T> xs, Func<T, TKey> keySelector, out TKey nextKey) where TKey : IComparable<TKey> {
			var currentKey = keySelector(xs.Current);
			while(xs.MoveNext()) { 
				nextKey = keySelector(xs.Current);
				var c = nextKey.CompareTo(currentKey);
				if(c == 0)
					continue;
				if(c > 0)
					return true;
				throw new InvalidOperationException("Input must be sorted");
			}
			nextKey = default(TKey);
			return false;
		}
	}

	[Describe(typeof(SequenceChangeDetector<,>))]
	public class SequenceComparerSpec
	{
		public void find_missing_elements() {
			var missing = new List<int>();
			var comparer = new SequenceChangeDetector<int,int> { 
				OnMissing = missing.Add,
			};
			var a = new [] { 1, 2, 3};
			var b = new [] { 2, 3, 4};

			comparer.FindChanges(a, id => id, b, id => id);
			Check.That(
				() => missing.Count == 1, 
				() => missing[0] == a.Except(b).Single());
		}

		public void find_extra_elements() {
			var extra = new List<int>();
			var comparer = new SequenceChangeDetector<int, int> {
				OnExtra= extra.Add,
			};
			var a = new[] { 1, 3, 4 };
			var b = new[] { 2, 3, 4 };

			comparer.FindChanges(a, id => id, b, id => id);
			Check.That(
				() => extra.Count == 1,
				() => extra[0] == b.Except(a).Single());
		}

		public void trailing_extras() {
			var extra = new List<int>();
			var comparer = new SequenceChangeDetector<int, int> {
				OnExtra = extra.Add,
			};
			var a = new[] { 1, 2 };
			var b = new[] { 1, 2, 3, 4 };

			comparer.FindChanges(a, id => id, b, id => id);
			Check.With(() => b.Except(a))
			.That(
				expected => expected.Count() == extra.Count,
				expected => expected.SequenceEqual(extra));
		}

		public void trailing_missing() {
			var missing = new List<int>();
			var comparer = new SequenceChangeDetector<int, int> {
				OnMissing = missing.Add,
			};
			var a = new[] { 1, 2, 3, 4};
			var b = new[] { 1, 2, };

			comparer.FindChanges(a, id => id, b, id => id);
			Check.With(() => a.Except(b))
			.That(
				expected => expected.Count() == missing.Count,
				expected => expected.SequenceEqual(missing));
		}

		public void sequence_key_types() {
			var knownIds = new [] { 1, 3 };
			var items = new[] { Item(1, "A"), Item(2, "B"), Item(3, "C") };

			var missing = new List<KeyValuePair<int, string>>();
			var comparer = new SequenceChangeDetector<KeyValuePair<int, string>, int> {
				OnMissing = missing.Add,
			};

			comparer.FindChanges(items, x => x.Key, knownIds, x => x);
			Check.That(() => missing.Count == 1, () => missing[0].Key == 2);
		}

		public void assumes_sorted_inputs() {
			var comparer = new SequenceChangeDetector<int, int>();
			var a = new[] { 2, 1, };
			var b = new[] { 1, 2, };

			Check.Exception<InvalidOperationException>(() => comparer.FindChanges(a, id => id, b, id => id));
			Check.Exception<InvalidOperationException>(() => comparer.FindChanges(b, id => id, a, id => id));
		}

		KeyValuePair<TKey, TValue> Item<TKey, TValue>(TKey key, TValue value) => new KeyValuePair<TKey, TValue>(key, value);
	}
}
