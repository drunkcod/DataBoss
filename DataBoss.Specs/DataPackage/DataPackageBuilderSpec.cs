using Cone;
using DataBoss.Data;

namespace DataBoss.DataPackage.Specs
{
	[Describe(typeof(DataPackage))]
	public class DataPackage_BuilderSpec
	{
		public void WithPrimaryKey_composite_key() {
			var dp = new DataPackage()
				.AddResource("my-resource", () => SequenceDataReader.Create(new[] { new { Id = 1, Value = "One" } }))
				.WithPrimaryKey("Id", "Value");
		}

		public void WithPrimaryKey_array_version() {
			var dp = new DataPackage()
				.AddResource("my-resource", () => SequenceDataReader.Create(new[]{ new { Id = 1, Value = "One" } }))
				.WithPrimaryKey(new[]{ "Id", "Value" });
		}
	}
}
