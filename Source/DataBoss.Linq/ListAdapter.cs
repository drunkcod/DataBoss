using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace DataBoss.Linq
{
	public static class ListAdapter
	{
		public static ListAdapter<TFrom, TTo> Create<TFrom, TTo>(IReadOnlyList<TFrom> source, Func<TFrom, TTo> selector) =>
			new ListAdapter<TFrom, TTo>(source, selector);
	}

	public class ListAdapter<TFrom, TTo> : IReadOnlyList<TTo>
	{
		readonly IReadOnlyList<TFrom> source;
		readonly Func<TFrom, TTo> selector;

		public ListAdapter(IReadOnlyList<TFrom> source, Func<TFrom, TTo> selector) {
			this.source = source;
			this.selector = selector;
		}

		public TTo this[int index] => selector(source[index]);
		public int Count => source.Count;

		public IEnumerator<TTo> GetEnumerator() => source.Select(selector).GetEnumerator();
		IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
	}
}
