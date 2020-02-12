using Newtonsoft.Json;

namespace DataBoss.DataPackage
{
	public class TabularDataSchemaFieldDescription
	{
		[JsonProperty("name")]
		public string Name;
		[JsonProperty("type")]
		public string Type;
	}
}
