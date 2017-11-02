using System;
using System.Collections.Generic;

namespace DataBoss
{
	public class SetDifference<TItem, TKnown>
	{
		public Action<TItem> OnMissing;
		public Action<TKnown> OnExtra;

		public void Symmetric<TKey>(IEnumerable<TItem> x, Func<TItem, TKey> keySelectorX , IEnumerable<TKnown> y, Func<TKnown, TKey> keySelectorY) where TKey : IComparable<TKey> {
			using(var xs = x.GetEnumerator())
			using(var ys = y.GetEnumerator()) { 
				var moreX = xs.MoveNext();
				var moreY = ys.MoveNext();
				if(moreX && moreY) {
					var keyX = keySelectorX(xs.Current);
					var keyY = keySelectorY(ys.Current);
					while (moreX && moreY) {
						var c = keyX.CompareTo(keyY);
						if (c == 0) { 
							moreX = MoveNextKey(xs, keySelectorX, out keyX);
							moreY = MoveNextKey(ys, keySelectorY, out keyY);
						} else if(c < 0) {
							OnMissing?.Invoke(xs.Current);
							moreX = MoveNextKey(xs, keySelectorX, out keyX);
						} else {
							OnExtra?.Invoke(ys.Current);
							moreY = MoveNextKey(ys, keySelectorY, out keyY);
						}
					}
				}
				if (moreX && OnMissing != null)
					do { OnMissing(xs.Current); } while ((MoveNextKey(xs, keySelectorX, out var _)));

				if (moreY && OnExtra != null)
					do { OnExtra(ys.Current); } while((MoveNextKey(ys, keySelectorY, out var _)));
			}
		}

		static bool MoveNextKey<T, TKey>(IEnumerator<T> xs, Func<T, TKey> keySelector, out TKey nextKey) where TKey : IComparable<TKey> {
			var currentKey = keySelector(xs.Current);
			if(xs.MoveNext()) { 
				nextKey = keySelector(xs.Current);
				var c = nextKey.CompareTo(currentKey);
				if(c >= 0)
					return true;
				throw new InvalidOperationException("Input must be sorted");
			}
			nextKey = default(TKey);
			return false;
		}
	}
}
