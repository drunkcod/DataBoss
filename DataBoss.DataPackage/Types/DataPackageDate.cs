using System;
using System.ComponentModel.DataAnnotations.Schema;
using System.Reflection;
using DataBoss.Data;

namespace DataBoss.DataPackage.Types
{
	[Field(SchemaType = "date")]
	[CustomAttributeProvider(typeof(DataPackageDateAttributes))]
	public struct DataPackageDate
	{
		readonly DateTime source;

		public DataPackageDate(DateTime source) {
			this.source = source;
		}

		public override int GetHashCode() => source.Date.GetHashCode();
		public override string ToString() => source.ToString("yyyy-MM-dd");
	}

	[AttributeUsage(AttributeTargets.Class | AttributeTargets.Struct)]
	public class FieldAttribute : Attribute
	{
		public string SchemaType;
	}

	public class DataPackageDateAttributes : ICustomAttributeProvider
	{
		readonly ColumnAttribute column = new ColumnAttribute { TypeName = "date" };

		public object[] GetCustomAttributes(bool inherit) => new[] { column };

		public object[] GetCustomAttributes(Type attributeType, bool inherit) {
			if(attributeType == typeof(ColumnAttribute))
				return new[]{ column };
			return Empty<object>.Array;
		}

		public bool IsDefined(Type attributeType, bool inherit) =>
			attributeType == typeof(ColumnAttribute);
	}

}
