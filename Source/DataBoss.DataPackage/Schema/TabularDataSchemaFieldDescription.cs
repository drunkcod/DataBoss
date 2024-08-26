using System.ComponentModel;
using System.Globalization;
using Newtonsoft.Json;

namespace DataBoss.DataPackage
{
	[method: JsonConstructor]
	public class TabularDataSchemaFieldDescription(
		string name,
		string type,
		string format = null,
		TabularDataSchemaFieldConstraints? constraints = null,
		string decimalChar = null)
	{
		internal static readonly NumberFormatInfo DefaultNumberFormat = new() {
			NumberDecimalSeparator = DefaultDecimalChar,
		};

		public const string DefaultDecimalChar = ".";

		[JsonProperty("name")]
		public readonly string Name = name;
		[JsonProperty("type")]
		public readonly string Type = type;
		[JsonProperty("format", NullValueHandling = NullValueHandling.Ignore)]
		public readonly string Format = format;

		[DefaultValue(DefaultDecimalChar), JsonProperty("decimalChar", NullValueHandling = NullValueHandling.Ignore)]
		public string DecimalChar => this.IsNumber() ? decimalChar ?? DefaultDecimalChar : null;

		[JsonProperty("constraints", DefaultValueHandling = DefaultValueHandling.Ignore)]
		public readonly TabularDataSchemaFieldConstraints? Constraints = constraints;
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
