using System;

namespace DataBoss
{
	[AttributeUsage(AttributeTargets.Method)]
	class DataBossCommandAttribute : Attribute
	{
		public DataBossCommandAttribute(string name) {
			this.Name = name;
		}

		public readonly string Name;
	}
}