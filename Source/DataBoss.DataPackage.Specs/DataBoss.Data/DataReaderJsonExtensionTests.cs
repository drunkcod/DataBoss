using System.Text.Json;
using CheckThat;
using DataBoss.Data;
using Xunit;

namespace DataBoss.Data
{
	public class DataReaderJsonExtensionTests
	{
		[Fact]
		public void super_duper_bonkers() {
			var rows = new[] { new { Id = 1, Value = "Hello" }, new { Id = 2, Value = "World." } };
			Check.That(() => SequenceDataReader.Create(rows).ToJson() == NewtonsoftSerialize(rows));
		}

		[Fact]
		public void null_field() {
			var rows = new[] { new { IsNull = (int?)null, }};
			Check.That(() => SequenceDataReader.Create(rows).ToJson() == NewtonsoftSerialize(rows));		
		}

		[Fact]
		public void newtonsoft_json_array() {
			var rows = new[] { new { Id = 1, Value = "Hello" }, new { Id = 2, Value = "World." } };
			Check.That(() => NewtonsoftSerialize(SequenceDataReader.Create(rows).ToJsonArray()) == NewtonsoftSerialize(rows));
		}

		[Fact]
		public void system_text_json_array() {
			var rows = new[] { new { Id = 1, Value = "Hello" }, new { Id = 2, Value = "World." } };
			Check.That(() => JsonSerializer.Serialize(SequenceDataReader.Create(rows).ToJsonArray(), null) == NewtonsoftSerialize(rows));
		}

		[Fact]
		public void columnar() {
			var rows = new[] { new { Id = (int?)1, Value = "Hello" }, new { Id = (int?)null, Value = "World." } };
			var expected = new { 
				Id = new[] { (int?)1, null },
				Value = new[] { "Hello", "World." },
			};
			Check.That(
				() => JsonSerializer.Serialize(SequenceDataReader.Create(rows).ToJsonColumns(), null) == NewtonsoftSerialize(expected),
				() => NewtonsoftSerialize(SequenceDataReader.Create(rows).ToJsonColumns()) == NewtonsoftSerialize(expected));
		}

		[Fact]
		public void single_object() {
			var item = new {
				Int32 = 1,
				String = "Hello",
				NullInt = (int?)null,
			};
			var r = SequenceDataReader.Items(item);
			r.Read();
			Check.That(
				() => JsonSerializer.Serialize(r.ToJsonObject(), null) == NewtonsoftSerialize(item),
				() => NewtonsoftSerialize(r.ToJsonObject()) == NewtonsoftSerialize(item));

		}

		static string NewtonsoftSerialize(object value) => Newtonsoft.Json.JsonConvert.SerializeObject(value);
	}
}
