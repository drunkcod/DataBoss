using System;

namespace DataBoss.Data
{
	[AttributeUsage(AttributeTargets.Method)]
	public class ConsiderAsCtorAttribute : Attribute
	{ }
}