using System;

namespace DataBoss.Data.Scripting
{
	[AttributeUsage(AttributeTargets.Property | AttributeTargets.Field)]
	public class ClusteredAttribute : Attribute { }
}
