using System;
using System.Linq;
using System.Reflection;

namespace DataBoss.Core
{
	static class CustomAttributeProviderExtensions
	{
		public static bool Any<T>(this ICustomAttributeProvider attributes) where T : Attribute {
			return attributes.GetCustomAttributes(typeof(T), true).Length != 0;
		}

		public static T Single<T>(this ICustomAttributeProvider attributes) where T : Attribute {
			return attributes.GetCustomAttributes(typeof(T), true).Cast<T>().Single();
		}

		public static T SingleOrDefault<T>(this ICustomAttributeProvider attributes) where T : Attribute {
			return attributes.GetCustomAttributes(typeof(T), true).Cast<T>().SingleOrDefault();
		}
	}
}