using System;
using System.Reflection;

namespace DataBoss.Data
{
	[AttributeUsage(AttributeTargets.Class | AttributeTargets.Struct)]
	public class CustomAttributeProviderAttribute : Attribute, ICustomAttributeProvider
	{
		ICustomAttributeProvider attributes;

		public CustomAttributeProviderAttribute(Type type) {
			this.attributes = (ICustomAttributeProvider)Activator.CreateInstance(type);
		}

		object[] ICustomAttributeProvider.GetCustomAttributes(bool inherit) =>
			attributes.GetCustomAttributes(inherit);

		object[] ICustomAttributeProvider.GetCustomAttributes(Type attributeType, bool inherit) =>
			attributes.GetCustomAttributes(attributeType, inherit);

		bool ICustomAttributeProvider.IsDefined(Type attributeType, bool inherit) =>
			attributes.IsDefined(attributeType, inherit);
	}
}