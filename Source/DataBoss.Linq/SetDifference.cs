using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace DataBoss
{
	public class SetDifference<TItem, TKnown>
	{
		public Action<TItem> OnMissing;
		public Action<TKnown> OnExtra;

		public void Symmetric<TKey>(IEnumerable<TItem> x, Func<TItem, TKey> keySelectorX, IEnumerable<TKnown> y, Func<TKnown, TKey> keySelectorY) where TKey : IComparable<TKey> {
			using var xs = x.GetEnumerator();
			using var ys = y.GetEnumerator();

			var moreX = xs.MoveNext();
			var moreY = ys.MoveNext();

			if (moreX && moreY) {
				var keyX = keySelectorX(xs.Current);
				var keyY = keySelectorY(ys.Current);
				while (moreX && moreY) {
					var c = keyX.CompareTo(keyY);
					if (c == 0) {
						moreX = MoveNextKey(xs, keySelectorX, ref keyX);
						moreY = MoveNextKey(ys, keySelectorY, ref keyY);
					}
					else if (c < 0) {
						OnMissing?.Invoke(xs.Current);
						moreX = MoveNextKey(xs, keySelectorX, ref keyX);
					}
					else {
						OnExtra?.Invoke(ys.Current);
						moreY = MoveNextKey(ys, keySelectorY, ref keyY);
					}
				}
			}
			if (moreX && OnMissing != null) {
				var key = keySelectorX(xs.Current);
				do { OnMissing(xs.Current); } while ((MoveNextKey(xs, keySelectorX, ref key)));
			}

			if (moreY && OnExtra != null) {
				var key = keySelectorY(ys.Current);
				do { OnExtra(ys.Current); } while ((MoveNextKey(ys, keySelectorY, ref key)));
			}
		}

		static bool MoveNextKey<T, TKey>(IEnumerator<T> xs, Func<T, TKey> keySelector, ref TKey currentKey) where TKey : IComparable<TKey> {
			while (xs.MoveNext()) {
				var nextKey = keySelector(xs.Current);
				switch (nextKey.CompareTo(currentKey)) {
					case 0: continue;
					case var c when c < 0: return InputNotSorted();
					default:
						currentKey = nextKey;
						return true;
				}
			}
			return false;
		}

		public async Task SymmetricAsync<TKey>(IAsyncEnumerable<TItem> x, Func<TItem, TKey> keySelectorX, IAsyncEnumerable<TKnown> y, Func<TKnown, TKey> keySelectorY) where TKey : IComparable<TKey> {
			var xs = x.GetAsyncEnumerator();
			var ys = y.GetAsyncEnumerator();

			var moreX = await xs.MoveNextAsync();
			var moreY = await ys.MoveNextAsync();

			if (moreX && moreY) {
				var keyX = keySelectorX(xs.Current);
				var keyY = keySelectorY(ys.Current);
				while (moreX && moreY) {
					var c = keyX.CompareTo(keyY);
					if (c == 0) {
						(moreX, keyX) = await MoveNextKeyAsync(xs, keySelectorX, keyX);
						(moreY, keyY) = await MoveNextKeyAsync(ys, keySelectorY, keyY);
					}
					else if (c < 0) {
						OnMissing?.Invoke(xs.Current);
						(moreX, keyX) = await MoveNextKeyAsync(xs, keySelectorX, keyX);
					}
					else {
						OnExtra?.Invoke(ys.Current);
						(moreY, keyY) = await MoveNextKeyAsync(ys, keySelectorY, keyY);
					}
				}
			}
			if (moreX && OnMissing != null) {
				var key = keySelectorX(xs.Current);
				var more = true;
				do {
					OnMissing(xs.Current);
					(more, key) = await MoveNextKeyAsync(xs, keySelectorX, key);
				} while (more);
			}

			if (moreY && OnExtra != null) {
				var key = keySelectorY(ys.Current);
				var more = true;
				do {
					OnExtra(ys.Current);
					(more, key) = await MoveNextKeyAsync(ys, keySelectorY, key);
				} while (more);
			}
		}

		static async Task<(bool Succes, TKey NextKey)> MoveNextKeyAsync<T, TKey>(IAsyncEnumerator<T> xs, Func<T, TKey> keySelector, TKey currentKey) where TKey : IComparable<TKey> {
			while (await xs.MoveNextAsync()) {
				var nextKey = keySelector(xs.Current);
				switch (nextKey.CompareTo(currentKey)) {
					case 0: continue;
					case var c when c < 0: return (InputNotSorted(), nextKey);
					default:
						return (true, nextKey);
				}
			}
			return (false, currentKey);
		}

		static bool InputNotSorted() => throw new InvalidOperationException("Input must be sorted");
	}
}
