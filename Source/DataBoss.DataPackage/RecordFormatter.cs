using System;
using System.Data;
using System.Globalization;
using DataBoss.DataPackage.Types;

namespace DataBoss.DataPackage
{
	class RecordFormatter
	{
		public static readonly RecordFormatter DefaultFormatter = new(TabularDataSchemaFieldDescription.DefaultNumberFormat);

		readonly NumberFormatInfo format;

		public RecordFormatter(NumberFormatInfo format) {
			this.format = format;
		}

		public Func<IDataRecord, int, string> GetFormatter(TabularDataSchemaFieldDescription field, Type fieldType) {
			var formatter = field.IsNumber() ? GetNumberFormatter(field) : this;
			return formatter.GetFormatterCore(field, fieldType);
		}

		RecordFormatter GetNumberFormatter(TabularDataSchemaFieldDescription field) {
			if (string.IsNullOrEmpty(field.DecimalChar))
				return DefaultFormatter;

			if (field.DecimalChar == format.NumberDecimalSeparator)
				return this;
			
			return new RecordFormatter(new NumberFormatInfo { NumberDecimalSeparator = field.DecimalChar });
		}

		Func<IDataRecord, int, string> GetFormatterCore(TabularDataSchemaFieldDescription field, Type fieldType) {
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

		Func<IDataRecord, int, string> int16;
		public Func<IDataRecord, int, string> FormatInt16 => int16 ??= (IDataRecord r, int i) => r.GetInt16(i).ToString(format);

		Func<IDataRecord, int, string> int32;
		public Func<IDataRecord, int, string> FormatInt32 => int32 ??= (IDataRecord r, int i) => r.GetInt32(i).ToString(format);

		Func<IDataRecord, int, string> int64;
		public Func<IDataRecord, int, string> FormatInt64 => int64 ??= (IDataRecord r, int i) => r.GetInt64(i).ToString(format);

		Func<IDataRecord, int, string> @float;
		public Func<IDataRecord, int, string> FormatFloat => @float ??= (IDataRecord r, int i) => r.GetFloat(i).ToString(format);

		Func<IDataRecord, int, string> @double;
		public Func<IDataRecord, int, string> FormatDouble => @double ??= (IDataRecord r, int i) => r.GetDouble(i).ToString(format);

		Func<IDataRecord, int, string> @decimal;
		public Func<IDataRecord, int, string> FormatDecimal => @decimal ??= (IDataRecord r, int i) => r.GetDouble(i).ToString(format);

		string FormatObject(IDataRecord r, int i) {
			var obj = r.GetValue(i);
			return obj is IFormattable x ? x.ToString(null, format) : obj?.ToString();
		}

		static readonly Func<IDataRecord, int, string> FormatDate = (IDataRecord r, int i) => ((DataPackageDate)r.GetDateTime(i)).ToString();
		static readonly Func<IDataRecord, int, string> FormatDateTime = (IDataRecord r, int i) => {
			var value = r.GetDateTime(i);
			if (value.Kind == DateTimeKind.Unspecified)
				throw new InvalidOperationException("DateTimeKind.Unspecified not supported.");
			return value.ToUniversalTime().ToString("yyyy'-'MM'-'dd'T'HH':'mm':'ssK");
		};

		static readonly Func<IDataRecord, int, string> FormatTimeSpan = (IDataRecord r, int i) => r.IsDBNull(i) ? null : ((TimeSpan)r.GetValue(i)).ToString("hh\\:mm\\:ss");
		static readonly Func<IDataRecord, int, string> FormatBinary = (IDataRecord r, int i) => r.IsDBNull(i) ? null : Convert.ToBase64String((byte[])r.GetValue(i));
		static readonly Func<IDataRecord, int, string> FormatString = (IDataRecord r, int i) => r.GetString(i);
		static readonly Func<IDataRecord, int, string> FormatBoolean = (IDataRecord r, int i) => r.GetBoolean(i).ToString();
		public static readonly Func<IDataRecord, int, string> FormatGuid = (IDataRecord r, int i) => r.GetGuid(i).ToString();
	}
}
