using System.Collections.Generic;
using System.Runtime.CompilerServices;

namespace DataBoss.Linq
{
	public static class KeyValuePair
	{
		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public static KeyValuePair<TKey, TValue> Create<TKey, TValue>(TKey key, TValue value) => new KeyValuePair<TKey, TValue>(key, value);
	}
}
