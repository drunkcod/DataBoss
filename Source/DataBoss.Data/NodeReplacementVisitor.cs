using System.Collections;
using System.Collections.Generic;
using System.Linq.Expressions;

namespace DataBoss.Linq.Expressions
{
	public class NodeReplacementVisitor : ExpressionVisitor, IEnumerable
	{
		readonly Dictionary<Expression, Expression> theReplacements = new Dictionary<Expression, Expression>();

		public void Add(Expression a, Expression b) => theReplacements.Add(a, b);

		public override Expression Visit(Expression node) {
			if (node == null)
				return null;

			if (theReplacements.TryGetValue(node, out var found))
				return found;
			return base.Visit(node);
		}

		IEnumerator IEnumerable.GetEnumerator() => theReplacements.GetEnumerator();

		public static Expression ReplaceParameters(LambdaExpression lambda, params Expression[] args) {
			var r = new NodeReplacementVisitor();
			for (var i = 0; i != args.Length; ++i)
				r.Add(lambda.Parameters[i], args[i]);
			return r.Visit(lambda.Body);
		}
	}
}