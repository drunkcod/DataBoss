using System.Collections.Generic;
using Newtonsoft.Json;

namespace DataBoss.DataPackage
{
	public struct DataPackageForeignKey
	{
		[JsonProperty("fields")]
		public IReadOnlyCollection<string> Fields;
		[JsonProperty("reference")]
		public readonly DataPackageKeyReference Reference;

		public DataPackageForeignKey(string field, DataPackageKeyReference reference) : this(new[]{ field }, reference) { }
		public DataPackageForeignKey(string[] fields, DataPackageKeyReference reference)
		{
			this.Fields = fields;
			this.Reference = reference;
		}
	}
}
