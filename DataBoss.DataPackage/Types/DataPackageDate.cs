using System;
using System.ComponentModel.DataAnnotations.Schema;
using System.Reflection;
using DataBoss.Data;

namespace DataBoss.DataPackage.Types
{
	[Field(SchemaType = "date")]
	[TypeMapping(TypeName = "date")]
	public struct DataPackageDate
	{
		public readonly DateTime Value;

		DataPackageDate(DateTime value) {
			this.Value = value.Date;
		}

		public override int GetHashCode() => Value.GetHashCode();
		public override string ToString() => Value.ToString("yyyy-MM-dd");

		public static explicit operator DateTime(DataPackageDate self) => self.Value;
		public static explicit operator DataPackageDate(DateTime source) => new DataPackageDate(source);
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
