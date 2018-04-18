using System;

namespace DataBoss.Linq
{
	public static class ArrayExtensions
	{
		public static TOutput[] ConvertAll<T, TOutput>(this T[] self, Converter<T, TOutput> converter) =>
			Array.ConvertAll(self, converter);

		public static T Single<T>(T[] ts) => ts.Length == 1 ? ts[0] : throw new InvalidOperationException("Array contains more than one element.");
	}
}