using System.Text.Json;
using CheckThat;
using DataBoss.Data;
using Xunit;

namespace DataBoss.DataBoss.Data
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
		public void newtonsoft_json_object() {
			var rows = new[] { new { Id = 1, Value = "Hello" }, new { Id = 2, Value = "World." } };
			Check.That(() => NewtonsoftSerialize(SequenceDataReader.Create(rows).ToJsonObjects()) == NewtonsoftSerialize(rows));
		}

		[Fact]
		public void system_text_json_object() {
			var rows = new[] { new { Id = 1, Value = "Hello" }, new { Id = 2, Value = "World." } };
			Check.That(() => JsonSerializer.Serialize(SequenceDataReader.Create(rows).ToJsonObjects(), null) == NewtonsoftSerialize(rows));
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

		static string NewtonsoftSerialize(object value) => Newtonsoft.Json.JsonConvert.SerializeObject(value);
	}
}
