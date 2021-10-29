using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace DataBoss.Linq
{
	class ArrayGrouping<TKey, TElement> : 
		IGrouping<TKey, TElement>,
		ICollection<TElement>, //Enables System.Linq.Enumerable fast-paths.
		IReadOnlyCollection<TElement>
	{
		readonly TElement[] items;
		readonly TKey key;

		public ArrayGrouping(TElement[] items, TKey key) {
			this.items = items;
			this.key = key;
		}

		public TKey Key => key;
		public int Count => items.Length;
		public bool IsReadOnly => true;

		public void Add(TElement item) => NotSupported();
		public void Clear() => NotSupported();
		public bool Remove(TElement item) => NotSupported();

		public bool Contains(TElement item) => Array.IndexOf(items, item) != -1;
		public void CopyTo(TElement[] array, int arrayIndex) => items.CopyTo(array, arrayIndex);

		public IEnumerator<TElement> GetEnumerator() => ((IEnumerable<TElement>)items).GetEnumerator();
		IEnumerator IEnumerable.GetEnumerator() => items.GetEnumerator();

		static bool NotSupported() => throw new InvalidOperationException("ArrayGrouping IsReadonly");
	}
}