using System;

namespace DataBoss.DataPackage
{
	[AttributeUsage(AttributeTargets.Class | AttributeTargets.Struct)]
	public class FieldAttribute : Attribute
	{
		public string SchemaType;
	}
}
