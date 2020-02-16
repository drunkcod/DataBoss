using Newtonsoft.Json;

namespace DataBoss.DataPackage
{
	public class TabularDataSchemaFieldDescription
	{
		[JsonProperty("name")]
		public string Name;
		[JsonProperty("type")]
		public string Type;

		[JsonProperty("decimalChar", DefaultValueHandling = DefaultValueHandling.Ignore)]
		public string DecimalChar;

		[JsonProperty("constraints", DefaultValueHandling = DefaultValueHandling.Ignore)]
		public TabularDataSchemaFieldConstraints Constraints;
	}

	public class TabularDataSchemaFieldConstraints
	{
		[JsonProperty("required", DefaultValueHandling = DefaultValueHandling.Ignore)]
		public bool IsRequired;
	}
}
