using System.Globalization;
using System.IO;
using System.Linq;
using Cone;
using DataBoss.Data;

namespace DataBoss.DataPackage.Specs
{
	[Describe(typeof(DataPackage))]
	public class DataPackageSpec
	{
		public void WithPrimaryKey_composite_key() {
			var dp = new DataPackage()
				.AddResource("my-resource", () => SequenceDataReader.Create(new { Id = 1, Value = "One" }))
				.WithPrimaryKey("Id", "Value");
		}

		public void WithPrimaryKey_array_version() {
			var dp = new DataPackage()
				.AddResource("my-resource", () => SequenceDataReader.Create(new { Id = 1, Value = "One" }))
				.WithPrimaryKey(new[]{ "Id", "Value" });
		}

		public void Save_normalizes_number_format() {
			var dp = new DataPackage();
			dp.AddResource("numbers", () => SequenceDataReader.Create(new { Value = 1.0 }));

			var xs = dp.Serialize(CultureInfo.GetCultureInfo("se-SV"));
			Check.That(
				() => GetNumbersFormat(xs).NumberDecimalSeparator == ",",
				() => GetNumbersFormat(xs.Serialize(CultureInfo.InvariantCulture)).NumberDecimalSeparator == ".",
				() => GetNumbersFormat(xs.Serialize(null)).NumberDecimalSeparator == ",");
		}

		NumberFormatInfo GetNumbersFormat(DataPackage data) => data.GetResource("numbers").Schema.Fields.Single().GetNumberFormat();
	}
}
