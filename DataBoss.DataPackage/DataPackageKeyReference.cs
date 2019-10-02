using System.Collections.Generic;
using Newtonsoft.Json;

namespace DataBoss.DataPackage
{
	public struct DataPackageKeyReference
	{
		[JsonProperty("resource")]
		public readonly string Resource;
		[JsonProperty("fields")]
		public IReadOnlyCollection<string> Fields;

		public DataPackageKeyReference(string resource, string field) : this(resource, new[]{ field }) { }
		public DataPackageKeyReference(string resource, string[] fields)
		{
			this.Resource = resource;
			this.Fields = fields;
		}
	}
}
