using CheckThat;
using Newtonsoft.Json;
using Xunit;

namespace DataBoss.DataPackage.Specs.Schema
{
	public class TabularDataSchemaFieldDescriptionSpec
	{
		[Fact]
		public void decimalSeparator_null_is_same_as_default() {
			var withNull = new TabularDataSchemaFieldDescription("field", "number", decimalChar: null);
			var withDefault = new TabularDataSchemaFieldDescription("field", "number", decimalChar: TabularDataSchemaFieldDescription.DefaultDecimalChar);

			Check.That(() => withNull.GetNumberFormat() == withDefault.GetNumberFormat());
		}

		[Fact]
		public void json_roundtrip() {
			var desc = new TabularDataSchemaFieldDescription(
				"field",
				"integer",
				constraints: new TabularDataSchemaFieldConstraints(required: true, maxLength: 4));

			var fromJson = FromJson(desc);

			Check.That(
				() => desc.Constraints.Value.IsRequired == fromJson.Constraints.Value.IsRequired,
				() => desc.Constraints.Value.MaxLength == fromJson.Constraints.Value.MaxLength);
		}

		public static T FromJson<T>(T value) =>
			JsonConvert.DeserializeObject<T>(JsonConvert.SerializeObject(value));
	}
}
