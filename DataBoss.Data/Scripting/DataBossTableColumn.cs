using System;
using System.Reflection;

namespace DataBoss.Data.Scripting
{
	public class DataBossTableColumn : ICustomAttributeProvider
	{
		readonly ICustomAttributeProvider attributes;

		public readonly string Name;
		public readonly DataBossDbType ColumnType;

		public DataBossTableColumn(DataBossDbType columnType, ICustomAttributeProvider attributes, string name) {
			this.ColumnType = columnType;
			this.attributes = attributes;
			this.Name = name;
		}

		public object[] GetCustomAttributes(Type attributeType, bool inherit) =>
			attributes.GetCustomAttributes(attributeType, inherit);

		public object[] GetCustomAttributes(bool inherit) =>
			attributes.GetCustomAttributes(inherit);

		public bool IsDefined(Type attributeType, bool inherit) =>
			attributes.IsDefined(attributeType, inherit);
	}
}