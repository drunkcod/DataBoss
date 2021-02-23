using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using CheckThat;
using Newtonsoft.Json;
using Xunit;

namespace DataBoss.Data.DataFrames
{
	public class DataFrameSpec {
		[Fact]
		public void create_from_DataReader() {
			var items = SequenceDataReader.Items(
				new { Id = 1, Message = "Hello", Value = (float?)42 },
				new { Id = 2, Message = "World", Value = default(float?) });
			var df = DataFrame.Create(items);

			Check.That(
				() => df.Columns.Count == items.FieldCount,
				() => df["Id"] is IEnumerable<int>,
				() => df["Message"] is IEnumerable<string>,
				() => df["Value"] is IEnumerable<float?>,
				() => df.Count == 2);
		}

		[Fact]
		public void column_gt() {
			var items = SequenceDataReader.Create(Enumerable
				.Range(0, 3)
				.Select(x => new { N = x }));
			var df = DataFrame.Create(items);
			var filter = df.GetColumn<int>("N") > 1;
			Check.That(
				() => filter.Count == 3,
				() => filter.SequenceEqual(new[] { false, false, true }));

		}

		[Fact]
		public void filter_column() {
			var items = SequenceDataReader.Create(Enumerable
				.Range(0, 10)
				.Select(x => new { N = x }));
			var df = DataFrame.Create(items);
			var filtered = df[df.GetColumn<int>("N") > 5];
			Check.That(() => filtered.Count == 4);
		}

		[Fact]
		public void DataSeries_() {
			var items = SequenceDataReader.Items(
				new { Id = 1, Message = "Hello", Value = (float?)42 },
				new { Id = 2, Message = "World", Value = default(float?) },
				new { Id = 3, Message = (string)null, Value = (float?)17.0f });

			var data = new DataSeriesReader(items);
			data.Add("Id");
			data.Add("Message");
			data.Add("Value");

			var series = data.Read();
			var ids = series[0];
			var messages = series[1];
			var values = series[2];
			
			Check.That(
				() => ids.Type == typeof(int),
				() => messages.Type == typeof(string),
				() => values.Type == typeof(float),
				() => ids.IsNull(0) == false,
				() => values.IsNull(1),
				() => (messages as DataSeries<string>)[1] == "World",
				() => values is IEnumerable<float?>,
				() => ToJson(messages) == "[\"Hello\",\"World\",null]",
				() => ToJson(values) == "[42.0,null,17.0]");
		}

		static string ToJson(object obj) => JsonConvert.SerializeObject(obj);
	}
}
