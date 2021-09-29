namespace DataBoss
{
	using System.Linq.Expressions;

	static class ExpressionExtensions
	{
		public static Expression Box(this Expression self) =>
			self.Type == typeof(object) ? self : Expression.Convert(self, typeof(object));
	}
}
