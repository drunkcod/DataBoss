using Newtonsoft.Json;

namespace DataBoss.DataPackage
{
	public class DataPackageResourceDescription
	{
		[JsonProperty("name")]
		public string Name;
		[JsonProperty("path")]
		public string Path;
		[JsonProperty("delimiter")]
		public string Delimiter;
		[JsonProperty("schema")]
		public DataPackageTabularSchema Schema;
	}
}
