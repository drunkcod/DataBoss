using System;

namespace DataBoss.Data
{
	public delegate bool RetryStrategy(int retryAttempt, Exception problem);

	public static class RetryStrategyExtensions
	{
		public static T Execute<T>(this RetryStrategy retry, Func<T> func) {
			for (var n = 1; ; ++n) {
				try {
					return func();
				} catch (Exception e) {
					if(!retry(n, e))
						throw;
				}
			}
		}
	}
}