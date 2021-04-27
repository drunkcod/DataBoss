using System;

namespace DataBoss.DataPackage
{
	public struct CsvNumber
	{
		public readonly string Value;
		public readonly IFormatProvider Format;

		public CsvNumber(string value, IFormatProvider format) {
			this.Value = value;
			this.Format = format;
		}

		public static explicit operator float(CsvNumber self) => float.Parse(self.Value, self.Format);
		public static explicit operator double(CsvNumber self) => double.Parse(self.Value, self.Format);
	}
}
