using System.ComponentModel;
using Newtonsoft.Json;

namespace DataBoss.DataPackage
{
	public class TabularDataSchemaFieldDescription
	{
		[JsonConstructor]
		public TabularDataSchemaFieldDescription(
			string name, 
			string type,
			TabularDataSchemaFieldConstraints constraints = null,
			string decimalChar = null) { 
			
			this.Name = name;
			this.Type = type;
			this.Constraints = constraints;
			this.DecimalChar = decimalChar;
		}
		
		[JsonProperty("name")]
		public readonly string Name;
		[JsonProperty("type")]
		public readonly string Type;

		[DefaultValue("."), JsonProperty("decimalChar", DefaultValueHandling = DefaultValueHandling.Ignore)]
		public readonly string DecimalChar;

		[JsonProperty("constraints", DefaultValueHandling = DefaultValueHandling.Ignore)]
		public readonly TabularDataSchemaFieldConstraints Constraints;
	}

	public class TabularDataSchemaFieldConstraints
	{
		[JsonProperty("required", DefaultValueHandling = DefaultValueHandling.Ignore)]
		public bool IsRequired;

		public override string ToString() => JsonConvert.SerializeObject(this);
	}

	public static class TabularDataSchemaFieldDescriptionExtensions
	{
		public static bool IsNumber(this TabularDataSchemaFieldDescription field) =>
			field.Type == "number";
	}
}
