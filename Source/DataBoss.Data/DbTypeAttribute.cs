using System;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Reflection;

namespace DataBoss.Data
{
	[AttributeUsage(AttributeTargets.Class | AttributeTargets.Struct)]
	public class DbTypeAttribute : Attribute, ICustomAttributeProvider
	{
		public readonly Type Type;
		readonly object[] customAttributes;
		public DbTypeAttribute(Type type) { this.Type = type; }
		public DbTypeAttribute(Type type, bool required) { 
			this.Type = type; 
			this.customAttributes = required ? new[] { new RequiredAttribute() } : null;
		}

		object[] ICustomAttributeProvider.GetCustomAttributes(bool inherit) => customAttributes;

		object[] ICustomAttributeProvider.GetCustomAttributes(Type attributeType, bool inherit) =>
			customAttributes is null ? null
			: customAttributes.Where(x => attributeType.IsAssignableFrom(x.GetType())).ToArray();

		bool ICustomAttributeProvider.IsDefined(Type attributeType, bool inherit) => 
			customAttributes is not null 
			&& Array.Exists(customAttributes, x => attributeType.IsAssignableFrom(x.GetType()));
	}
}