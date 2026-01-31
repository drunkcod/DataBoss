using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using DataBoss.Data;
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

		public static string Date(IDataRecord r, int i, NumberFormatInfo _) => ((DateOnly)r.GetValue(i)).ToString("yyyy-MM-dd");

		public static string DateTime(IDataRecord r, int i, NumberFormatInfo _) {
			var value = r.GetDateTime(i);
			if (value.Kind == DateTimeKind.Unspecified)
				throw new InvalidOperationException("DateTimeKind.Unspecified not supported.");
			return value.ToUniversalTime().ToString("yyyy'-'MM'-'dd'T'HH':'mm':'ssK");
		}

		public static string DateTimeOffset(IDataRecord r, int i, NumberFormatInfo _) =>
			r.GetFieldValue<DateTimeOffset>(i).ToString(@"yyyy-MM-dd HH:mm:ss.FFFFFFF zzz");

		public static string? TimeSpan(IDataRecord r, int i, NumberFormatInfo _) => r.IsDBNull(i) ? null : ((TimeSpan)r.GetValue(i)).ToString("hh\\:mm\\:ss");

		public static string? Object(IDataRecord r, int i, NumberFormatInfo format) {
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

		public readonly StringViewFormatter GetFormatter(Type fieldType) {
			switch (Type.GetTypeCode(fieldType)) {
				default:
					if (fieldType == typeof(TimeSpan))
						return FormatTimeSpan;
					if (fieldType == typeof(DateOnly))
						return FormatDate;
					if (fieldType == typeof(DateTimeOffset))
						return FormatDateTimeOffset;
					if (fieldType == typeof(byte[]))
						return FormatBinary;
					if (fieldType == typeof(Guid))
						return FormatGuid;
					return FormatObject;

				case TypeCode.DateTime: return FormatDateTime;

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
	}

	public interface IStringRecord
	{
		int FieldCount { get; }
		bool IsDBNull(int i);
		string? GetString(int i);
		string GetName(int i);
		object GetValue(int i);
	}

	readonly struct DataReaderStringView : IStringRecord
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
		readonly IDataReader source;

		DataReaderStringView(IDataReader source, (StringViewFormatter, NumberFormatInfo)[] formatField) {
			this.formatField = formatField;
			this.source = source;
		}

		public RecordStringViewFormat Format() => new(formatField);

		public int FieldCount => formatField.Length;

		public readonly bool Read() => source.Read();
		public readonly string GetName(int i) => source.GetName(i);
		public readonly object GetValue(int i) => source.GetValue(i);
		public readonly bool IsDBNull(int i) => source.IsDBNull(i);

		public readonly string? GetString(int i) {
			var (getter, format) = formatField[i];
			return getter(source, i, format);
		}

		public static DataReaderStringView Create(IReadOnlyList<TabularDataSchemaFieldDescription> outputFields, IDataReader data, CultureInfo? culture = null) => Create(outputFields, data, DefaultFormat, culture);
		public static DataReaderStringView Create(IReadOnlyList<TabularDataSchemaFieldDescription> outputFields, IDataReader data, in DataRecordStringViewFormatOptions options, CultureInfo? culture = null) =>
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

		static DataReaderStringView Create(IReadOnlyList<TabularDataSchemaFieldDescription> outputFields, IDataReader data, in DataRecordStringViewFormat format, CultureInfo? culture = null) {
			var defaultNumberFormat = culture?.NumberFormat ?? TabularDataSchemaFieldDescription.DefaultNumberFormat;
			var formatField = new (StringViewFormatter, NumberFormatInfo)[outputFields.Count];
			for (var i = 0; i != outputFields.Count; ++i)
				formatField[i] = (format.GetFormatter(data.GetFieldType(i)), GetNumberFormat(outputFields[i], defaultNumberFormat));

			return new DataReaderStringView(data, formatField);
		}

		static NumberFormatInfo GetNumberFormat(TabularDataSchemaFieldDescription field, NumberFormatInfo defaultFormat) {
			if (string.IsNullOrEmpty(field.DecimalChar))
				return TabularDataSchemaFieldDescription.DefaultNumberFormat;

			if (field.DecimalChar == defaultFormat.NumberDecimalSeparator)
				return defaultFormat;

			return new NumberFormatInfo { NumberDecimalSeparator = field.DecimalChar };
		}
	}

	struct RecordStringViewFormat((StringViewFormatter, NumberFormatInfo)[] formatField)
	{
		readonly (StringViewFormatter, NumberFormatInfo)[] formatField = formatField;
		public IDataRecord? Current;

		public readonly RecordStringView NewItemView() => new(formatField);


	}

	struct RecordStringView((StringViewFormatter, NumberFormatInfo)[] formatField) : IStringRecord
	{
		readonly (StringViewFormatter, NumberFormatInfo)[] formatField = formatField;
		public IDataRecord? Current;

		public readonly int FieldCount => Current!.FieldCount;
		public readonly string GetName(int i) => Current!.GetName(i);
		public readonly object GetValue(int i) => Current!.GetValue(i);
		public readonly bool IsDBNull(int i) => Current!.IsDBNull(i);

		public readonly string? GetString(int i) {
			var (getter, format) = formatField[i];
			return getter(Current!, i, format);
		}

	}
}
