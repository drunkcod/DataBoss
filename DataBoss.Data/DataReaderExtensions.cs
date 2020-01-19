using System;
using System.Data;
using System.Linq.Expressions;

namespace DataBoss.Data
{
	public static class DataReaderExtensions
	{
		public static void ForAll<TReader, T>(this TReader self, Expression<Action<T>> action) where TReader : IDataReader => ForAllCore(self, action);
		public static void ForAll<TReader, T1, T2>(this TReader self, Expression<Action<T1, T2>> action) where TReader : IDataReader => ForAllCore(self, action);
		public static void ForAll<TReader, T1, T2, T3>(this TReader self, Expression<Action<T1, T2, T3>> action) where TReader : IDataReader => ForAllCore(self, action);
		public static void ForAll<TReader, T1, T2, T3, T4>(this TReader self, Expression<Action<T1, T2, T3, T4>> action) where TReader : IDataReader => ForAllCore(self, action);
		public static void ForAll<TReader, T1, T2, T3, T4, T5>(this TReader self, Expression<Action<T1, T2, T3, T4, T5>> action) where TReader : IDataReader => ForAllCore(self, action);
		public static void ForAll<TReader, T1, T2, T3, T4, T5, T6>(this TReader self, Expression<Action<T1, T2, T3, T4, T5, T6>> action) where TReader : IDataReader => ForAllCore(self, action);
		public static void ForAll<TReader, T1, T2, T3, T4, T5, T6, T7>(this TReader self, Expression<Action<T1, T2, T3, T4, T5, T6, T7>> action) where TReader : IDataReader => ForAllCore(self, action);

		static void ForAllCore<TReader>(TReader self, LambdaExpression lambda) where TReader : IDataReader {
			var converter = ConverterFactory.Default.GetConverter(self, lambda);
			var act = (Action<TReader>)converter.Compile();
			while (self.Read())
				act(self);
		}
	}
}
