using Newtonsoft.Json;

namespace DataBoss.DataPackage
{
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
}
