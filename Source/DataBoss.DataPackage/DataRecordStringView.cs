using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using DataBoss.DataPackage.Types;

namespace DataBoss.DataPackage
{
	class DataRecordStringView
	{
		readonly (Func<IDataRecord, int, NumberFormatInfo, string>, NumberFormatInfo)[] formatField;

		DataRecordStringView((Func<IDataRecord, int, NumberFormatInfo, string>, NumberFormatInfo)[] formatField) {
			this.formatField = formatField;
		}

		public string GetString(IDataRecord r, int i) {
			var (getter, format) = formatField[i];
			return getter(r, i, format);
		}

		public static DataRecordStringView Create(IReadOnlyList<TabularDataSchemaFieldDescription> outputFields, IDataReader data, CultureInfo culture = null) {
			var defaultNumberFormat = culture?.NumberFormat ?? TabularDataSchemaFieldDescription.DefaultNumberFormat;
			var formatField = new (Func<IDataRecord, int, NumberFormatInfo, string>, NumberFormatInfo)[outputFields.Count];
			for (var i = 0; i != outputFields.Count; ++i)
				formatField[i] = (GetFormatter(outputFields[i], data.GetFieldType(i)), GetNumberFormat(outputFields[i], defaultNumberFormat));

			return new DataRecordStringView(formatField);
		}

		static NumberFormatInfo GetNumberFormat(TabularDataSchemaFieldDescription field, NumberFormatInfo defaultFormat) {
			if (string.IsNullOrEmpty(field.DecimalChar))
				return TabularDataSchemaFieldDescription.DefaultNumberFormat;

			if (field.DecimalChar == defaultFormat.NumberDecimalSeparator)
				return defaultFormat;

			return new NumberFormatInfo { NumberDecimalSeparator = field.DecimalChar };
		}

		public static Func<IDataRecord, int, NumberFormatInfo, string> GetFormatter(TabularDataSchemaFieldDescription field, Type fieldType) {
			switch (Type.GetTypeCode(fieldType)) {
				default:
					if (fieldType == typeof(TimeSpan))
						return FormatTimeSpan;
					if (fieldType == typeof(byte[]))
						return FormatBinary;
					if (fieldType == typeof(Guid))
						return FormatGuid;
					return FormatObject;

				case TypeCode.DateTime:
					if (field.Type == "date")
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

		public static readonly Func<IDataRecord, int, NumberFormatInfo, string> FormatInt16 =
			(IDataRecord r, int i, NumberFormatInfo format) => r.GetInt16(i).ToString(format);

		public static readonly Func<IDataRecord, int, NumberFormatInfo, string> FormatInt32 =
			(IDataRecord r, int i, NumberFormatInfo format) => r.GetInt32(i).ToString(format);

		public static readonly Func<IDataRecord, int, NumberFormatInfo, string> FormatInt64 =
			(IDataRecord r, int i, NumberFormatInfo format) => r.GetInt64(i).ToString(format);

		public static readonly Func<IDataRecord, int, NumberFormatInfo, string> FormatFloat =
			(IDataRecord r, int i, NumberFormatInfo format) => r.GetFloat(i).ToString(format);

		public static readonly Func<IDataRecord, int, NumberFormatInfo, string> FormatDouble =
			(IDataRecord r, int i, NumberFormatInfo format) => r.GetDouble(i).ToString(format);

		public static readonly Func<IDataRecord, int, NumberFormatInfo, string> FormatDecimal =
			(IDataRecord r, int i, NumberFormatInfo format) => r.GetDouble(i).ToString(format);

		static readonly Func<IDataRecord, int, NumberFormatInfo, string> FormatObject =
			(IDataRecord r, int i, NumberFormatInfo format) => {
				var obj = r.GetValue(i);
				return obj is IFormattable x ? x.ToString(null, format) : obj?.ToString();
			};

		static readonly Func<IDataRecord, int, NumberFormatInfo, string> FormatDate =
			(IDataRecord r, int i, NumberFormatInfo _) => ((DataPackageDate)r.GetDateTime(i)).ToString();

		static readonly Func<IDataRecord, int, NumberFormatInfo, string> FormatDateTime =
			(IDataRecord r, int i, NumberFormatInfo _) => {
				var value = r.GetDateTime(i);
				if (value.Kind == DateTimeKind.Unspecified)
					throw new InvalidOperationException("DateTimeKind.Unspecified not supported.");
				return value.ToUniversalTime().ToString("yyyy'-'MM'-'dd'T'HH':'mm':'ssK");
			};

		static readonly Func<IDataRecord, int, NumberFormatInfo, string> FormatTimeSpan =
			(IDataRecord r, int i, NumberFormatInfo _) => r.IsDBNull(i) ? null : ((TimeSpan)r.GetValue(i)).ToString("hh\\:mm\\:ss");

		static readonly Func<IDataRecord, int, NumberFormatInfo, string> FormatBinary =
			(IDataRecord r, int i, NumberFormatInfo _) => r.IsDBNull(i) ? null : Convert.ToBase64String((byte[])r.GetValue(i));

		static readonly Func<IDataRecord, int, NumberFormatInfo, string> FormatString =
			(IDataRecord r, int i, NumberFormatInfo _) => r.GetString(i);

		static readonly Func<IDataRecord, int, NumberFormatInfo, string> FormatBoolean =
			(IDataRecord r, int i, NumberFormatInfo _) => r.GetBoolean(i).ToString();

		public static readonly Func<IDataRecord, int, NumberFormatInfo, string> FormatGuid =
			(IDataRecord r, int i, NumberFormatInfo _) => r.GetGuid(i).ToString();
	}
}
