using System;

namespace DataBoss.Data
{
	[AttributeUsage(AttributeTargets.Class | AttributeTargets.Struct)]
	public class DbTypeAttribute : Attribute
	{
		public readonly Type Type;
		public DbTypeAttribute(Type type) { this.Type = type; }
	}
}