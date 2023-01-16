using System;

namespace DataBoss.Linq
{
	public static class Collection
	{
		public static EmptyCollection<T> Empty<T>() => new();

		public static T[] ArrayInit<T>(int size, Func<int, T> getValue) {
			var xs = new T[size];
			for(var i = 0; i != size; ++i)
				xs[i] = getValue(i);
			return xs;
		}
	}
}