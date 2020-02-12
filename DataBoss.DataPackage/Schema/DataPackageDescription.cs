using System.Collections.Generic;
using System.IO;
using Newtonsoft.Json;

namespace DataBoss.DataPackage
{
	public class DataPackageDescription
	{
		[JsonProperty("resources")]
		public List<DataPackageResourceDescription> Resources = new List<DataPackageResourceDescription>();

		public static DataPackageDescription Load(string path) => JsonConvert.DeserializeObject<DataPackageDescription>(File.ReadAllText(path));
	}
}
