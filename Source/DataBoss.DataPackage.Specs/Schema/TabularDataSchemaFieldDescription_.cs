using CheckThat;
using Newtonsoft.Json;
using Xunit;

namespace DataBoss.DataPackage.Schema
{
	public class TabularDataSchemaFieldDescription_
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

		[Fact]
		public void decimalSeparator_default_for_number() => 
			Check.That(() => new TabularDataSchemaFieldDescription("x", "number", null, null, null).DecimalChar == TabularDataSchemaFieldDescription.DefaultDecimalChar);

		[Fact]
		public void decimalSeparator_default_for_non_number() => 
			Check.That(() => new TabularDataSchemaFieldDescription("x", "string", null, null, null).DecimalChar == null);

		public static T FromJson<T>(T value) =>
			JsonConvert.DeserializeObject<T>(JsonConvert.SerializeObject(value));
	}
}
