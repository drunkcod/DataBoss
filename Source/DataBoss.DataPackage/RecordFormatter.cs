using System;
using System.Data;
using DataBoss.DataPackage.Types;

namespace DataBoss.DataPackage
{
	class RecordFormatter
	{
		readonly IFormatProvider format;

		public RecordFormatter(IFormatProvider format) {
			this.format = format;
		}

		public Func<IDataRecord, int, string> GetFormatter(Type type, TabularDataSchemaFieldDescription fieldDescription) {
			switch (Type.GetTypeCode(type)) {
				default:
					if (type == typeof(TimeSpan))
						return FormatTimeSpan;
					if (type == typeof(byte[]))
						return FormatBinary;
					if (type == typeof(Guid))
						return FormatGuid;
					return FormatObject;

				case TypeCode.DateTime:
					if (fieldDescription.Type == "date")
						return FormatDate;
					return FormatDateTime;

				case TypeCode.String: return FormatString;
				case TypeCode.Boolean: return FormatBoolean;

				case TypeCode.Int16: return FormatInt16;
				case TypeCode.Int32: return FormatInt32;
				case TypeCode.Int64: return FormatInt64;

				case TypeCode.Single: return FormatFloat;
				case TypeCode.Double: return FormatDouble;
				case TypeCode.Decimal: return FormatDecimal;
			}
		}

		public string FormatInt16(IDataRecord r, int i) => r.GetInt16(i).ToString(format);
		public string FormatInt32(IDataRecord r, int i) => r.GetInt32(i).ToString(format);
		public string FormatInt64(IDataRecord r, int i) => r.GetInt64(i).ToString(format);
		public string FormatFloat(IDataRecord r, int i) => r.GetFloat(i).ToString(format);
		public string FormatDouble(IDataRecord r, int i) => r.GetDouble(i).ToString(format);
		public string FormatDecimal(IDataRecord r, int i) => r.GetDecimal(i).ToString(format);

		string FormatObject(IDataRecord r, int i) {
			var obj = r.GetValue(i);
			return obj is IFormattable x ? x.ToString(null, format) : obj?.ToString();
		}

		static string FormatDate(IDataRecord r, int i) => ((DataPackageDate)r.GetDateTime(i)).ToString();
		static string FormatDateTime(IDataRecord r, int i) {
			var value = r.GetDateTime(i);
			if (value.Kind == DateTimeKind.Unspecified)
				throw new InvalidOperationException("DateTimeKind.Unspecified not supported.");
			return value.ToUniversalTime().ToString("yyyy'-'MM'-'dd'T'HH':'mm':'ssK");
		}

		static string FormatTimeSpan(IDataRecord r, int i) => r.IsDBNull(i) ? null : ((TimeSpan)r.GetValue(i)).ToString("hh\\:mm\\:ss");
		static string FormatBinary(IDataRecord r, int i) => r.IsDBNull(i) ? null : Convert.ToBase64String((byte[])r.GetValue(i));
		static string FormatString(IDataRecord r, int i) => r.GetString(i);
		static string FormatBoolean(IDataRecord r, int i) => r.GetBoolean(i).ToString();
		public string FormatGuid(IDataRecord r, int i) => r.GetGuid(i).ToString();
	}
}
