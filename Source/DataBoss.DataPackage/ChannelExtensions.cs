using System;
using System.Collections.Generic;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace DataBoss.DataPackage
{
	static class ChannelExtensions
	{
		public static void Write<T>(this ChannelWriter<T> w, T item) {
			do {
				if (w.TryWrite(item))
					return;
			} while (WaitToWrite(w));
		}

		public static bool WaitToWrite<T>(this ChannelWriter<T> w) => 
			GetResult(w.WaitToWriteAsync());

		public static bool WaitToRead<T>(this ChannelReader<T> r) =>
			GetResult(r.WaitToReadAsync());

		static T GetResult<T>(ValueTask<T> x) => x.IsCompletedSuccessfully ? x.Result : x.AsTask().GetAwaiter().GetResult();

		public static void ForEach<T>(this ChannelReader<T> r, Action<T> action) {
			do {
				while (r.TryRead(out var item))
					action(item);
			} while (r.WaitToRead());
		}

		public static IEnumerable<T> GetConsumingEnumerable<T>(this ChannelReader<T> r) {
			do {
				while (r.TryRead(out var item))
					yield return item;
			} while (r.WaitToRead());
		}
	}
}
