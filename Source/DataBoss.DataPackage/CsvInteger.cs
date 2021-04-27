using System;

namespace DataBoss.DataPackage
{
	public struct CsvInteger
	{
		public readonly string Value;
		public readonly IFormatProvider Format;

		public CsvInteger(string value, IFormatProvider format) {
			this.Value = value;
			this.Format = format;
		}

		public static explicit operator short(CsvInteger self) => short.Parse(self.Value, self.Format);
		public static explicit operator int(CsvInteger self) => int.Parse(self.Value, self.Format);
		public static explicit operator long(CsvInteger self) => long.Parse(self.Value, self.Format);
	}
}
