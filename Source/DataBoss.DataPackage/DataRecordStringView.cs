using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using DataBoss.DataPackage.Types;

namespace DataBoss.DataPackage
{
	readonly struct DataRecordStringView
	{
		readonly (Func<IDataRecord, int, NumberFormatInfo, string>, NumberFormatInfo)[] formatField;

		DataRecordStringView((Func<IDataRecord, int, NumberFormatInfo, string>, NumberFormatInfo)[] formatField) {
			this.formatField = formatField;
		}

		public int FieldCount => formatField.Length;

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

		static readonly Func<IDataRecord, int, NumberFormatInfo, string> FormatString = StringFrom.String;
		static readonly Func<IDataRecord, int, NumberFormatInfo, string> FormatBoolean = StringFrom.Boolean;

		static readonly Func<IDataRecord, int, NumberFormatInfo, string> FormatInt16 = StringFrom.Int16;
		static readonly Func<IDataRecord, int, NumberFormatInfo, string> FormatInt32 = StringFrom.Int32;
		static readonly Func<IDataRecord, int, NumberFormatInfo, string> FormatInt64 = StringFrom.Int64;

		static readonly Func<IDataRecord, int, NumberFormatInfo, string> FormatFloat = StringFrom.Float;
		static readonly Func<IDataRecord, int, NumberFormatInfo, string> FormatDouble = StringFrom.Double;
		static readonly Func<IDataRecord, int, NumberFormatInfo, string> FormatDecimal = StringFrom.Decimal;

		static readonly Func<IDataRecord, int, NumberFormatInfo, string> FormatDate = StringFrom.Date;
		static readonly Func<IDataRecord, int, NumberFormatInfo, string> FormatDateTime = StringFrom.DateTime;
		static readonly Func<IDataRecord, int, NumberFormatInfo, string> FormatTimeSpan = StringFrom.TimeSpan;
		static readonly Func<IDataRecord, int, NumberFormatInfo, string> FormatBinary = StringFrom.Binary;
		static readonly Func<IDataRecord, int, NumberFormatInfo, string> FormatGuid = StringFrom.Guid;
		static readonly Func<IDataRecord, int, NumberFormatInfo, string> FormatObject = StringFrom.Object;

		static class StringFrom
		{
			public static string Int16(IDataRecord r, int i, NumberFormatInfo format) => r.GetInt16(i).ToString(format);
			public static string Int32(IDataRecord r, int i, NumberFormatInfo format) => r.GetInt32(i).ToString(format);
			public static string Int64(IDataRecord r, int i, NumberFormatInfo format) => r.GetInt64(i).ToString(format);

			public static string Float(IDataRecord r, int i, NumberFormatInfo format) => r.GetFloat(i).ToString(format);
			public static string Double(IDataRecord r, int i, NumberFormatInfo format) => r.GetDouble(i).ToString(format);
			public static string Decimal(IDataRecord r, int i, NumberFormatInfo format) => r.GetDouble(i).ToString(format);
			
			public static string Date(IDataRecord r, int i, NumberFormatInfo _) => ((DataPackageDate)r.GetDateTime(i)).ToString();

			public static string DateTime(IDataRecord r, int i, NumberFormatInfo _) {
				var value = r.GetDateTime(i);
				if (value.Kind == DateTimeKind.Unspecified)
					throw new InvalidOperationException("DateTimeKind.Unspecified not supported.");
				return value.ToUniversalTime().ToString("yyyy'-'MM'-'dd'T'HH':'mm':'ssK");
			}

			public static string TimeSpan(IDataRecord r, int i, NumberFormatInfo _) => r.IsDBNull(i) ? null : ((TimeSpan)r.GetValue(i)).ToString("hh\\:mm\\:ss");

			public static string Object(IDataRecord r, int i, NumberFormatInfo format) {
				var obj = r.GetValue(i);
				return obj is IFormattable x ? x.ToString(null, format) : obj?.ToString();
			}

			public static string Boolean(IDataRecord r, int i, NumberFormatInfo _) => r.GetBoolean(i).ToString();
			public static string Binary(IDataRecord r, int i, NumberFormatInfo _) => r.IsDBNull(i) ? null : Convert.ToBase64String((byte[])r.GetValue(i));
			public static string String(IDataRecord r, int i, NumberFormatInfo _) => r.GetString(i);
			public static string Guid(IDataRecord r, int i, NumberFormatInfo _) => r.GetGuid(i).ToString();
		}
	}
}
