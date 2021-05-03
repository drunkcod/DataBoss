using System.Collections.Generic;
using System.Linq;
using Newtonsoft.Json;

namespace DataBoss.DataPackage
{
	public class TabularDataSchema
	{
		[JsonProperty("fields")]
		public List<TabularDataSchemaFieldDescription> Fields;

		[JsonProperty("primaryKey", DefaultValueHandling = DefaultValueHandling.Ignore)]
		[JsonConverter(typeof(ItemOrArrayJsonConverter))]
		public List<string> PrimaryKey;

		[JsonProperty("foreignKeys", DefaultValueHandling = DefaultValueHandling.Ignore)]
		public List<DataPackageForeignKey> ForeignKeys;

		public TabularDataSchema Clone() => new TabularDataSchema {
			Fields = Fields?.ToList(),
			PrimaryKey = PrimaryKey?.ToList(),
			ForeignKeys = ForeignKeys?.ToList(),
		};
	}
}
