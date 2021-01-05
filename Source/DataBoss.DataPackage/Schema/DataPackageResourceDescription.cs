using Newtonsoft.Json;

namespace DataBoss.DataPackage
{
	public class DataPackageResourceDescription
	{
		[JsonProperty("name")]
		public string Name;
		[JsonProperty("path")]
		public string Path;

		[JsonProperty("dialect")]
		public CsvDialectDescription Dialect;

		[JsonProperty("schema")]
		public TabularDataSchema Schema;
	}

	public class CsvDialectDescription
	{
		[JsonProperty("delimiter")]
		public string Delimiter;

	}
}
