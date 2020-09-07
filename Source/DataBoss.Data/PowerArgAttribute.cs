using System;

namespace DataBoss
{
	[AttributeUsage(AttributeTargets.Field | AttributeTargets.Property, AllowMultiple = false)]
	public class PowerArgAttribute : Attribute
	{
		int? sortOrder;
		public int Order { get { return sortOrder.GetValueOrDefault(); } set { sortOrder = value; } }
		public string Hint { get; set; }

		public int? GetSortOrder() => sortOrder;
	}
}