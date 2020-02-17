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

			var bytes = new MemoryStream();
			dp.SaveZip(bytes, CultureInfo.GetCultureInfo("se-SV"));

			var xs = DataPackage.LoadZip(() => new MemoryStream(bytes.ToArray()));
			Check.That(() => xs.GetResource("numbers").Schema.Fields.Single().DecimalChar == ",");

			var xBytes = new MemoryStream();
			xs.SaveZip(xBytes, CultureInfo.InvariantCulture);
			var invariant = DataPackage.LoadZip(() => new MemoryStream(xBytes.ToArray()));
			Check.That(() => invariant.GetResource("numbers").Schema.Fields.Single().DecimalChar == ".");

			var yBytes = new MemoryStream();
			xs.SaveZip(yBytes);
			var keep = DataPackage.LoadZip(() => new MemoryStream(yBytes.ToArray()));
			Check.That(() => keep.GetResource("numbers").Schema.Fields.Single().DecimalChar == ",");
		}
	}
}
