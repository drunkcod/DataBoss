using System.ComponentModel;
using System.Globalization;
using Newtonsoft.Json;

namespace DataBoss.DataPackage
{
	public class TabularDataSchemaFieldDescription
	{
		internal static readonly NumberFormatInfo DefaultNumberFormat = new() {
			NumberDecimalSeparator = TabularDataSchemaFieldDescription.DefaultDecimalChar,
		};

		public const string DefaultDecimalChar = ".";

		[JsonConstructor]
		public TabularDataSchemaFieldDescription(
			string name, 
			string type,
			string format = null,
			TabularDataSchemaFieldConstraints? constraints = null,
			string decimalChar = null) { 
			
			this.Name = name;
			this.Type = type;
			this.Format = format;
			this.Constraints = constraints;
			this.DecimalChar = decimalChar;
		}
		
		[JsonProperty("name")]
		public readonly string Name;
		[JsonProperty("type")]
		public readonly string Type;
		[JsonProperty("format", NullValueHandling = NullValueHandling.Ignore)]
		public readonly string Format;

		[DefaultValue(DefaultDecimalChar), JsonProperty("decimalChar", DefaultValueHandling = DefaultValueHandling.Ignore, NullValueHandling = NullValueHandling.Ignore)]
		public readonly string DecimalChar;

		[JsonProperty("constraints", DefaultValueHandling = DefaultValueHandling.Ignore)]
		public readonly TabularDataSchemaFieldConstraints? Constraints;
	}

	public static class TabularDataSchemaFieldDescriptionExtensions
	{
		public static bool IsRequired(this TabularDataSchemaFieldDescription field) =>
			field.Constraints?.IsRequired ?? false;

		public static bool IsNumber(this TabularDataSchemaFieldDescription field) =>
			field.Type == "number";

		public static TabularDataSchemaFieldDescription WithDecimalChar(this TabularDataSchemaFieldDescription field, string decimalChar) => 
			new (
				name: field.Name,
				type: field.Type,
				format: field.Format,
				constraints: field.Constraints,
				decimalChar: decimalChar);

		public static NumberFormatInfo GetNumberFormat(this TabularDataSchemaFieldDescription field) => 
			(string.IsNullOrEmpty(field.DecimalChar) || field.DecimalChar == TabularDataSchemaFieldDescription.DefaultNumberFormat.NumberDecimalSeparator)
			? TabularDataSchemaFieldDescription.DefaultNumberFormat
			: new NumberFormatInfo { NumberDecimalSeparator = field.DecimalChar };
	}
}
