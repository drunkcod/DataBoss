using System;

namespace DataBoss
{
	[AttributeUsage(AttributeTargets.Method)]
	public class DataBossCommandAttribute : Attribute
	{
		public DataBossCommandAttribute(string name) {
			this.Name = name;
		}

		public readonly string Name;
	}
}