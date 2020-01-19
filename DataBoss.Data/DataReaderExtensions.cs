using System;
using System.Data;

namespace DataBoss.Data
{
	public static class DataReaderExtensions
	{
		public static void ForAll<TReader, T>(this TReader self, Action<T> action) where TReader : IDataReader => CallForAll(self, action);
		public static void ForAll<TReader, T1, T2>(this TReader self, Action<T1, T2> action) where TReader : IDataReader => CallForAll(self, action);
		public static void ForAll<TReader, T1, T2, T3>(this TReader self, Action<T1, T2, T3> action) where TReader : IDataReader => CallForAll(self, action);
		public static void ForAll<TReader, T1, T2, T3, T4>(this TReader self, Action<T1, T2, T3, T4> action) where TReader : IDataReader => CallForAll(self, action);
		public static void ForAll<TReader, T1, T2, T3, T4, T5>(this TReader self, Action<T1, T2, T3, T4, T5> action) where TReader : IDataReader => CallForAll(self, action);
		public static void ForAll<TReader, T1, T2, T3, T4, T5, T6>(this TReader self, Action<T1, T2, T3, T4, T5, T6> action) where TReader : IDataReader => CallForAll(self, action);
		public static void ForAll<TReader, T1, T2, T3, T4, T5, T6, T7>(this TReader self, Action<T1, T2, T3, T4, T5, T6, T7> action) where TReader : IDataReader => CallForAll(self, action);

		static void CallForAll<TReader, TTarget>(TReader self, TTarget target) where TReader : IDataReader where TTarget : Delegate {
			var converter = ConverterFactory.Default.GetTrampoline(self, target);
			var jump = (Action<TReader, TTarget>)converter.Compile();
			while (self.Read())
				jump(self, target);
		}
	}
}
