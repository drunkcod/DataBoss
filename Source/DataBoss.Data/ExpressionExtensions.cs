namespace DataBoss.Expressions
{
	using System.Linq.Expressions;

	public static class ExpressionExtensions
	{
		public static Expression Box(this Expression self) =>
			self.Type == typeof(object) ? self : Expression.Convert(self, typeof(object));
	}
}
