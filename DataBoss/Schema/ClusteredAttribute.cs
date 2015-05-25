using System;

namespace DataBoss.Schema
{
	[AttributeUsage(AttributeTargets.Property | AttributeTargets.Field)]
	public class ClusteredAttribute : Attribute { }
}
