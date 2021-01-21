using System.Text.Json;
using CheckThat;
using CheckThat.Helpers;
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
			Check.That(() => NewtonsoftSerialize(SequenceDataReader.Create(rows).ToJsonObject()) == NewtonsoftSerialize(rows));
		}

		[Fact]
		public void system_text_json_object() {
			var rows = new[] { new { Id = 1, Value = "Hello" }, new { Id = 2, Value = "World." } };
			Check.That(() => JsonSerializer.Serialize(SequenceDataReader.Create(rows).ToJsonObject(), null) == NewtonsoftSerialize(rows));
		}

		static string NewtonsoftSerialize(object value) => Newtonsoft.Json.JsonConvert.SerializeObject(value);

		[Fact]
		public void closes_reader() {
			var closed = new ActionSpy();
			var rows = new DataReaderDecorator(SequenceDataReader.Items(new { Message = "Hello World." }));
			rows.Closed += closed;
			JsonSerializer.Serialize(rows.ToJsonObject());
			Check.That(() => closed.HasBeenCalled);
		}
	}
}
