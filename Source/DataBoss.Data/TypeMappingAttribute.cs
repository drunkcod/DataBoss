using System;

namespace DataBoss.Data
{
	[AttributeUsage(AttributeTargets.Class | AttributeTargets.Struct)]
	public class TypeMappingAttribute : Attribute
	{
		public string TypeName;
	}
}