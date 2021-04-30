using System.Collections.Generic;
using Newtonsoft.Json;

namespace DataBoss.DataPackage
{
	public struct DataPackageForeignKey
	{
		[JsonProperty("fields")]
		[JsonConverter(typeof(ItemOrArrayJsonConverter))]
		public IReadOnlyList<string> Fields { get; }

		[JsonProperty("reference")]
		public DataPackageKeyReference Reference { get; }

		public DataPackageForeignKey(string field, DataPackageKeyReference reference) : this(new[]{ field }, reference) { }

		[JsonConstructor]
		public DataPackageForeignKey(
			[JsonConverter(typeof(ItemOrArrayJsonConverter))] string[] fields,
			DataPackageKeyReference reference)
		{
			this.Fields = fields;
			this.Reference = reference;
		}
	}
}
