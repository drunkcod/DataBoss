using System;
using DataBoss.Data;

namespace DataBoss.DataPackage.Types
{
	[Field(SchemaType = "date")]
	[TypeMapping(TypeName = "date")]
	public readonly struct DataPackageDate
	{
		public readonly DateOnly Value;

		DataPackageDate(DateTime value) {
			this.Value = DateOnly.FromDateTime(value.Date);
		}
		DataPackageDate(DateOnly value) {
			this.Value = value;
		}

		public override readonly int GetHashCode() => Value.GetHashCode();
		public override readonly string ToString() => Value.ToString("yyyy-MM-dd");
		public static explicit operator DateTime(DataPackageDate self) => self.Value.ToDateTime(TimeOnly.MinValue);
		public static explicit operator DataPackageDate(DateOnly source) => new(source);
		public static explicit operator DataPackageDate(DateTime source) => new(source);
	}
}
