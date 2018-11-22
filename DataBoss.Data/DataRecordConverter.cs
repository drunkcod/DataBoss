using System;
using System.Linq.Expressions;

namespace DataBoss.Data
{
	public class DataRecordConverter
	{
		Delegate compiled;

		public readonly LambdaExpression Expression;
		public Delegate Compiled => compiled ?? (compiled = Expression.Compile());

		public DataRecordConverter(LambdaExpression expression) {
			this.Expression = expression;
			this.compiled = null;
		}

		public DataRecordConverter<TReader, T> ToTyped<TReader,T>() => new DataRecordConverter<TReader, T>(this);
	}

	public struct DataRecordConverter<TReader, T>
	{
		readonly DataRecordConverter converter;

		internal DataRecordConverter(DataRecordConverter converter) {
			this.converter = converter;
		}

		public Expression<Func<TReader, T>> Expression => (Expression<Func<TReader, T>>)converter.Expression;
		public Func<TReader, T> Compiled => (Func<TReader, T>)converter.Compiled;
	}
}