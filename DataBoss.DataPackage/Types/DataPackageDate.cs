using System;
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
}
