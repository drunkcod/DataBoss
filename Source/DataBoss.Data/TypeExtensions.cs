using System;

namespace DataBoss
{
	public static class TypeExtensions
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
