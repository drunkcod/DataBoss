using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace DataBoss
{
	static class ExpressionExtensions
	{
		public static Expression Box(this Expression self) =>
			self.Type == typeof(object) ? self : Expression.Convert(self, typeof(object));
	}
}
