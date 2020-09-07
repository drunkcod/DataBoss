using System.Collections.Generic;
using Newtonsoft.Json;

namespace DataBoss.DataPackage
{
	public struct DataPackageKeyReference
	{
		[JsonProperty("resource")]
		public string Resource { get; }
		[JsonProperty("fields")]
		[JsonConverter(typeof(ItemOrArrayJsonConverter))]
		public IReadOnlyCollection<string> Fields { get; }

		public DataPackageKeyReference(string resource, string field) : this(resource, new[]{ field }) { }
		
		[JsonConstructor]
		public DataPackageKeyReference(
			string resource,
			[JsonConverter(typeof(ItemOrArrayJsonConverter))] string[] fields)
		{
			this.Resource = resource;
			this.Fields = fields;
		}
	}
}
