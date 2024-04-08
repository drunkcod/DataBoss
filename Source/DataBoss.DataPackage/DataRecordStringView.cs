using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using DataBoss.Data;
using DataBoss.DataPackage.Types;

#nullable enable

namespace DataBoss.DataPackage
{
	public delegate string? StringViewFormatter(IDataRecord record, int n, NumberFormatInfo formatInfo);

	static class DataPackageStringFrom
	{
		public static string Int16(IDataRecord r, int i, NumberFormatInfo format) => r.GetInt16(i).ToString(format);
		public static string Int32(IDataRecord r, int i, NumberFormatInfo format) => r.GetInt32(i).ToString(format);
		public static string Int64(IDataRecord r, int i, NumberFormatInfo format) => r.GetInt64(i).ToString(format);

		public static string Float(IDataRecord r, int i, NumberFormatInfo format) => r.GetFloat(i).ToString(format);
		public static string Double(IDataRecord r, int i, NumberFormatInfo format) => r.GetDouble(i).ToString(format);
		public static string Decimal(IDataRecord r, int i, NumberFormatInfo format) => r.GetDecimal(i).ToString(format);
		
		public static string Date(IDataRecord r, int i, NumberFormatInfo _) => ((DataPackageDate)r.GetDateTime(i)).ToString();

		public static string DateTime(IDataRecord r, int i, NumberFormatInfo _) {
			var value = r.GetDateTime(i);
			if (value.Kind == DateTimeKind.Unspecified)
				throw new InvalidOperationException("DateTimeKind.Unspecified not supported.");
			return value.ToUniversalTime().ToString("yyyy'-'MM'-'dd'T'HH':'mm':'ssK");
		}

		public static string DateTimeOffset(IDataRecord r, int i, NumberFormatInfo _) =>
			r.GetFieldValue<DateTimeOffset>(i).ToString(@"yyyy-MM-dd HH:mm:ss.FFFFFFF zzz");

		public static string? TimeSpan(IDataRecord r, int i, NumberFormatInfo _) => r.IsDBNull(i) ? null : ((TimeSpan)r.GetValue(i)).ToString("hh\\:mm\\:ss");

		public static string Object(IDataRecord r, int i, NumberFormatInfo format) {
			var obj = r.GetValue(i);
			return obj is IFormattable x ? x.ToString(null, format) : obj?.ToString();
		}

		public static string Boolean(IDataRecord r, int i, NumberFormatInfo _) => r.GetBoolean(i).ToString();
		public static string? Binary(IDataRecord r, int i, NumberFormatInfo _) => r.IsDBNull(i) ? null : Convert.ToBase64String((byte[])r.GetValue(i));
		public static string String(IDataRecord r, int i, NumberFormatInfo _) => r.GetString(i);
		public static string Guid(IDataRecord r, int i, NumberFormatInfo _) => r.GetGuid(i).ToString();
	}

	public struct DataRecordStringViewFormatOptions
	{
		public StringViewFormatter? FormatString;
		public StringViewFormatter? FormatBoolean;

		public StringViewFormatter? FormatInt16;
		public StringViewFormatter? FormatInt32;
		public StringViewFormatter? FormatInt64;

		public StringViewFormatter? FormatFloat;
		public StringViewFormatter? FormatDouble;
		public StringViewFormatter? FormatDecimal;

		public StringViewFormatter? FormatDate;
		public StringViewFormatter? FormatDateTime;
		public StringViewFormatter? FormatDateTimeOffset;
		public StringViewFormatter? FormatTimeSpan;
		public StringViewFormatter? FormatBinary;
		public StringViewFormatter? FormatGuid;
		public StringViewFormatter? FormatObject;
	}

	struct DataRecordStringViewFormat
	{
		public StringViewFormatter FormatString;
		public StringViewFormatter FormatBoolean;

		public StringViewFormatter FormatInt16;
		public StringViewFormatter FormatInt32;
		public StringViewFormatter FormatInt64;

		public StringViewFormatter FormatFloat;
		public StringViewFormatter FormatDouble;
		public StringViewFormatter FormatDecimal;

		public StringViewFormatter FormatDate;
		public StringViewFormatter FormatDateTime;
		public StringViewFormatter FormatDateTimeOffset;
		public StringViewFormatter FormatTimeSpan;
		public StringViewFormatter FormatBinary;
		public StringViewFormatter FormatGuid;
		public StringViewFormatter FormatObject;
	}

	readonly struct DataRecordStringView
	{
		static readonly DataRecordStringViewFormat DefaultFormat = new() {
			FormatString = DataPackageStringFrom.String,
			FormatBoolean = DataPackageStringFrom.Boolean,

			FormatInt16 = DataPackageStringFrom.Int16,
			FormatInt32 = DataPackageStringFrom.Int32,
			FormatInt64 = DataPackageStringFrom.Int64,

			FormatFloat = DataPackageStringFrom.Float,
			FormatDouble = DataPackageStringFrom.Double,
			FormatDecimal = DataPackageStringFrom.Decimal,

			FormatDate = DataPackageStringFrom.Date,
			FormatDateTime = DataPackageStringFrom.DateTime,
			FormatDateTimeOffset = DataPackageStringFrom.DateTimeOffset,
			FormatTimeSpan = DataPackageStringFrom.TimeSpan,
			FormatBinary = DataPackageStringFrom.Binary,
			FormatGuid = DataPackageStringFrom.Guid,
			FormatObject = DataPackageStringFrom.Object,
		};

		readonly (StringViewFormatter, NumberFormatInfo)[] formatField;

		DataRecordStringView((StringViewFormatter, NumberFormatInfo)[] formatField) {
			this.formatField = formatField;
		}

		public int FieldCount => formatField.Length;

		public string? GetString(IDataRecord r, int i) {
			var (getter, format) = formatField[i];
			return getter(r, i, format);
		}

		public static DataRecordStringView Create(IReadOnlyList<TabularDataSchemaFieldDescription> outputFields, IDataReader data, CultureInfo? culture = null) => Create(outputFields, data, DefaultFormat, culture);
		public static DataRecordStringView Create(IReadOnlyList<TabularDataSchemaFieldDescription> outputFields, IDataReader data, in DataRecordStringViewFormatOptions options, CultureInfo? culture = null) => 
			Create(outputFields, data, new DataRecordStringViewFormat {
				FormatString = options.FormatString ?? DefaultFormat.FormatString,
				FormatBoolean = options.FormatBoolean ?? DefaultFormat.FormatBoolean,

				FormatInt16 = options.FormatInt16 ?? DefaultFormat.FormatInt16,
				FormatInt32 = options.FormatInt32 ?? DefaultFormat.FormatInt32,
				FormatInt64 = options.FormatInt64 ?? DefaultFormat.FormatInt64,

				FormatFloat = options.FormatFloat ?? DefaultFormat.FormatFloat,
				FormatDouble = options.FormatDouble ?? DefaultFormat.FormatDouble,
				FormatDecimal = options.FormatDecimal ?? DefaultFormat.FormatDecimal,

				FormatDate = options.FormatDate ?? DefaultFormat.FormatDate,
				FormatDateTime = options.FormatDateTime ?? DefaultFormat.FormatDateTime,
				FormatDateTimeOffset = options.FormatDateTimeOffset ?? DefaultFormat.FormatDateTimeOffset,
				FormatTimeSpan = options.FormatTimeSpan ?? DefaultFormat.FormatTimeSpan,

				FormatBinary = options.FormatBinary ?? DefaultFormat.FormatBinary,
				FormatGuid = options.FormatGuid ?? DefaultFormat.FormatGuid,
				FormatObject = options.FormatObject ?? DefaultFormat.FormatObject,
			}, culture);

		static DataRecordStringView Create(IReadOnlyList<TabularDataSchemaFieldDescription> outputFields, IDataReader data, in DataRecordStringViewFormat format, CultureInfo? culture = null) {
			var defaultNumberFormat = culture?.NumberFormat ?? TabularDataSchemaFieldDescription.DefaultNumberFormat;
			var formatField = new (StringViewFormatter, NumberFormatInfo)[outputFields.Count];
			for (var i = 0; i != outputFields.Count; ++i)
				formatField[i] = (GetFormatter(outputFields[i], data.GetFieldType(i), format), GetNumberFormat(outputFields[i], defaultNumberFormat));

			return new DataRecordStringView(formatField);
		}

		static NumberFormatInfo GetNumberFormat(TabularDataSchemaFieldDescription field, NumberFormatInfo defaultFormat) {
			if (string.IsNullOrEmpty(field.DecimalChar))
				return TabularDataSchemaFieldDescription.DefaultNumberFormat;

			if (field.DecimalChar == defaultFormat.NumberDecimalSeparator)
				return defaultFormat;

			return new NumberFormatInfo { NumberDecimalSeparator = field.DecimalChar };
		}

		public static StringViewFormatter GetFormatter(TabularDataSchemaFieldDescription field, Type fieldType, in DataRecordStringViewFormat format) {
			switch (Type.GetTypeCode(fieldType)) {
				default:
					if (fieldType == typeof(TimeSpan))
						return format.FormatTimeSpan;
					if (fieldType == typeof(DateTimeOffset))
						return format.FormatDateTimeOffset;
					if (fieldType == typeof(byte[]))
						return format.FormatBinary;
					if (fieldType == typeof(Guid))
						return format.FormatGuid;
					return format.FormatObject;

				case TypeCode.DateTime:
					if (field.Type == "date")
						return format.FormatDate;
					return format.FormatDateTime;

				case TypeCode.String: return format.FormatString;
				case TypeCode.Boolean: return format.FormatBoolean;

				case TypeCode.Int16: return format.FormatInt16;
				case TypeCode.Int32: return format.FormatInt32;
				case TypeCode.Int64: return format.FormatInt64;

				case TypeCode.Single: return format.FormatFloat;
				case TypeCode.Double: return format.FormatDouble;
				case TypeCode.Decimal: return format.FormatDecimal;
			}
		}
	}
}
