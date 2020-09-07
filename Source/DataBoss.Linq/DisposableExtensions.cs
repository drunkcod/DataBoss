using System;

namespace DataBoss
{
	public static class DisposableExtensions
	{
		public static void Use<T>(this T x, Action<T> @do) where T : IDisposable {
			try { @do(x); }
			finally { x.Dispose(); }
		}

		public static TResult Use<T, TResult>(this T x, Func<T, TResult> @do) where T : IDisposable {
			try { return @do(x); }
			finally { x.Dispose(); }
		}
	}
}