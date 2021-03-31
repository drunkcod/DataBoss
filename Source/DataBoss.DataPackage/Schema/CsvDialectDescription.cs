using System.ComponentModel;
using Newtonsoft.Json;

namespace DataBoss.DataPackage.Schema
{
	public class CsvDialectDescription
	{
		public const string DefaultDelimiter = ",";
		[JsonProperty("delimiter"), DefaultValue(DefaultDelimiter)]
		public string Delimiter;

		[JsonProperty("header"), DefaultValue(true)]
		public bool HasHeaderRow;

		public static CsvDialectDescription GetDefaultDialect() => new() { 
			Delimiter = DefaultDelimiter,
			HasHeaderRow = true,
		};
	}
}
