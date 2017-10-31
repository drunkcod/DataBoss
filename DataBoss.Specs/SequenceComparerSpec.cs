using System;
using System.Collections.Generic;
using System.Linq;
using Cone;

namespace DataBoss.Specs
{
	class SequenceSetComparer
	{
		static Action<int> Nop => x => { };

		public Action<int> OnMissing = Nop;
		public Action<int> OnExtra = Nop;

		public void FindChanges(IEnumerable<int> a, Func<int, int> keySelectorA , IEnumerable<int> b, Func<int, int> keySelectorB) {
			using(var xs = a.GetEnumerator())
			using(var ys = b.GetEnumerator()) { 
				var moreA = xs.MoveNext();
				var moreB = ys.MoveNext();
				var keyA = keySelectorA(xs.Current);
				var keyB = keySelectorB(ys.Current);
				while(moreA && moreB) {
					if(keyA == keyB) { 
						moreA = MoveNextKey(xs, keySelectorA, out keyA);
						moreB = MoveNextKey(ys, keySelectorB, out keyB);
					} else if(keyA < keyB) {
						OnMissing(xs.Current);
						moreA = MoveNextKey(xs, keySelectorA, out keyA);
					} else {
						OnExtra(ys.Current);
						moreB = MoveNextKey(ys, keySelectorB, out keyB);
					}
				}
				if (moreA)
					do { OnExtra(xs.Current); } while ((MoveNextKey(xs, keySelectorA, out keyA)));

				if (moreB)
					do { OnExtra(ys.Current); } while((MoveNextKey(ys, keySelectorB, out keyB)));
			}
		}

		static bool MoveNextKey(IEnumerator<int> xs, Func<int, int> keySelector, out int nextKey) {
			var currentKey = keySelector(xs.Current);
			while(xs.MoveNext()) { 
				nextKey = keySelector(xs.Current);
				if (nextKey != currentKey) 
					return true;
			}
			nextKey = default(int);
			return false;
		}
	}

	[Describe(typeof(SequenceSetComparer))]
	public class SequenceComparerSpec
	{
		public void find_missing_elements() {
			var missing = new List<int>();
			var comparer = new SequenceSetComparer { 
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
			var comparer = new SequenceSetComparer {
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
			var comparer = new SequenceSetComparer {
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
			var extra = new List<int>();
			var comparer = new SequenceSetComparer {
				OnExtra = extra.Add,
			};
			var a = new[] { 1, 2, 3, 4};
			var b = new[] { 1, 2, };

			comparer.FindChanges(a, id => id, b, id => id);
			Check.With(() => a.Except(b))
			.That(
				expected => expected.Count() == extra.Count,
				expected => expected.SequenceEqual(extra));
		}
	}
}
