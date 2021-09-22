using System;
using System.ComponentModel.DataAnnotations;
using System.Reflection;

namespace DataBoss.DataPackage
{
	class RequiredAttributeProvider : ICustomAttributeProvider
	{
		readonly RequiredAttribute[] RequiredAttribute = new[] { new RequiredAttribute() };

		RequiredAttributeProvider() { }

		public static readonly RequiredAttributeProvider Instance = new();

		public object[] GetCustomAttributes(bool inherit) => RequiredAttribute;

		public object[] GetCustomAttributes(Type attributeType, bool inherit) =>
			attributeType == typeof(RequiredAttribute) ? RequiredAttribute : Array.Empty<object>();

		public bool IsDefined(Type attributeType, bool inherit) =>
			attributeType == typeof(RequiredAttribute);
	}
}
