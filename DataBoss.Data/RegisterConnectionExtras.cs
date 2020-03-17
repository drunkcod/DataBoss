using System;
using DataBoss.Data;

namespace DataBoss.Data
{
	[AttributeUsage(AttributeTargets.Assembly)]
	public class RegisterConnectionExtras : Attribute
	{
		public readonly Type ConnectionType;
		public readonly Type ExtrasType;

		public RegisterConnectionExtras(Type connectionType, Type extrasType) {
			this.ConnectionType = connectionType;
			this.ExtrasType = extrasType;
		}
	}
}