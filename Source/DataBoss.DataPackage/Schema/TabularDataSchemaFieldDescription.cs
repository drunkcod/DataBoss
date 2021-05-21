using System.ComponentModel;
using System.Globalization;
using Newtonsoft.Json;

namespace DataBoss.DataPackage
{
	public class TabularDataSchemaFieldDescription
	{
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

	public readonly struct TabularDataSchemaFieldConstraints
	{
		[JsonConstructor]
		public TabularDataSchemaFieldConstraints(bool required, int? maxLength = null) {
			this.IsRequired = required;
			this.MaxLength = maxLength;
		}

		[JsonProperty("required", DefaultValueHandling = DefaultValueHandling.Ignore)]
		public readonly bool IsRequired;

		[JsonProperty("maxLength", DefaultValueHandling = DefaultValueHandling.Ignore)]
		public readonly int? MaxLength;

		public override string ToString() => JsonConvert.SerializeObject(this);
	}

	public static class TabularDataSchemaFieldDescriptionExtensions
	{
		static readonly NumberFormatInfo DefaultNumberFormat = new NumberFormatInfo {
			NumberDecimalSeparator = TabularDataSchemaFieldDescription.DefaultDecimalChar,
		};

		public static bool IsRequired(this TabularDataSchemaFieldDescription field) =>
			field.Constraints?.IsRequired ?? false;

		public static bool IsNumber(this TabularDataSchemaFieldDescription field) =>
			field.Type == "number";

		public static NumberFormatInfo GetNumberFormat(this TabularDataSchemaFieldDescription field) => 
			(string.IsNullOrEmpty(field.DecimalChar) || field.DecimalChar == DefaultNumberFormat.NumberDecimalSeparator)
			? DefaultNumberFormat
			: new NumberFormatInfo { NumberDecimalSeparator = field.DecimalChar };
	}
}
