using System;
using System.Collections.Generic;
using System.ComponentModel;
using Newtonsoft.Json;

namespace DataBoss.DataPackage
{
	public class DataPackageResourceDescription
	{
		[JsonProperty("name")]
		public string Name;
		[JsonProperty("path"), JsonConverter(typeof(ItemOrArrayJsonConverter))]
		public IReadOnlyList<string> Path;
		[JsonProperty("format")]
		public string Format;

		[Obsolete("Use Dialect.Delimiter instead."), JsonProperty("delimiter")]
		public string Delimiter {
			set => (Dialect ??= new CsvDialectDescription()).Delimiter = value;
		}

		[JsonProperty("dialect")]
		public CsvDialectDescription Dialect;

		[JsonProperty("schema")]
		public TabularDataSchema Schema;
	}

	public class CsvDialectDescription
	{
		[JsonProperty("delimiter")]
		public string Delimiter;

		[JsonProperty("header"), DefaultValue(true)]
		public bool HasHeaderRow = true;
	}
}
