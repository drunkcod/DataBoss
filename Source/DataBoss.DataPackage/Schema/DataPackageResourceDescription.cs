using System;
using Newtonsoft.Json;

namespace DataBoss.DataPackage.Schema
{
	public class DataPackageResourceDescription
	{
		[JsonProperty("name")]
		public string Name;
		[JsonProperty("path", DefaultValueHandling = DefaultValueHandling.Ignore)]
		public ResourcePath Path;
		[JsonProperty("format")]
		public string Format;

		[Obsolete("Use Dialect.Delimiter instead."), JsonProperty("delimiter")]
		public string Delimiter {
			set => Dialect.Delimiter = value;
		}

		[JsonProperty("dialect")]
		public CsvDialectDescription Dialect = CsvDialectDescription.GetDefaultDialect();

		[JsonProperty("schema")]
		public TabularDataSchema Schema;
	}
}
