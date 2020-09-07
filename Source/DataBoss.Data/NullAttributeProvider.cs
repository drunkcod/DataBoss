using System;
using System.Reflection;

namespace DataBoss.Data
{
	public class NullAttributeProvider : ICustomAttributeProvider
	{
		NullAttributeProvider() { }

		public object[] GetCustomAttributes(bool inherit) => Empty<object>.Array;

		public object[] GetCustomAttributes(Type attributeType, bool inherit) => Empty<object>.Array;

		public bool IsDefined(Type attributeType, bool inherit) => false;

		public static readonly NullAttributeProvider Instance = new NullAttributeProvider();
	}
}