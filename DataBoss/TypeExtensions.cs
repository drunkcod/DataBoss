using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataBoss
{
	static class TypeExtensions
	{
		public static bool IsNullable(this Type t) => t.IsGenericType && t.GetGenericTypeDefinition() == typeof(Nullable<>);

		public static bool TryGetNullableTargetType(this Type t, out Type type) {
			if(t.IsNullable()) {
				type = t.GenericTypeArguments[0];
				return true;
			}
			type = null;
			return false;
		}
	}
}