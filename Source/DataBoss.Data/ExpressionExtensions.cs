namespace DataBoss.Expressions
{
	using System;
	using System.Linq.Expressions;
	using System.Xml;

	public static class ExpressionExtensions
	{
		public static Expression Box(this Expression self) =>
			self.Type.IsValueType ? Expression.Convert(self, typeof(object)) : self;

		public static Expression Convert(this Expression self, Type target) {
			if(self.NodeType == ExpressionType.Convert) {
				var c = (UnaryExpression)self;
				if(c.Method == null)
					return Convert(c.Operand, target);
			}
			return self.Type == target ? self : Expression.Convert(self, target);
		}
	}
}
