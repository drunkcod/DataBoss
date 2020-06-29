using System;
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

		static T GetResult<T>(ValueTask<T> x) => x.IsCompletedSuccessfully ? x.Result : x.AsTask().Result;

		public static void ForEach<T>(this ChannelReader<T> r, Action<T> action) {
			do {
				while (r.TryRead(out var item))
					action(item);
			} while (r.WaitToRead());
		}
	}
}
