using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using CheckThat;
using DataBoss.Data;
using DataBoss.DataPackage.Types;
using Newtonsoft.Json;
using Xunit;

namespace DataBoss.DataPackage
{
	public class DataPackageSpec
	{
		struct IdValueRow
		{
			public int Id;
			public string Value;
		}

		[Fact]
		public void AddResource_with_non_default_path() {
			var dp = new DataPackage()
				.AddResource(new DataPackageResourceOptions {
					Name = "my-resource",
					Path = "some/path.csv",
				}, () => SequenceDataReader.Items(new { Id = 1, Value = "One" }));

			var r = dp.Serialize();

			Check.That(() => r.Resources[0].Path.Single() == "some/path.csv");
		}

		[Fact]
		public void multi_part_resource() {
			var dp = new DataPackage();
			dp.AddResource(new DataPackageResourceOptions {
				Name = "1",
				Path = "parts/1.csv",
			}, () => SequenceDataReader.Items(new { Id = 1, Value = "One" }));
			
			dp.AddResource(new DataPackageResourceOptions {
				Name = "2",
				Path = "parts/2.csv",
				HasHeaderRow = false,
			}, () => SequenceDataReader.Items(new { Id = 2, Value = "Two" }));

			dp.AddResource(new DataPackageResourceOptions {
				Name = "all-parts",
				Path = new[] {
					"parts/1.csv",
					"parts/2.csv",
				},
			});

			var r = dp.Serialize();

			var allRows = r.GetResource("1").Read<IdValueRow>().Concat(r.GetResource("2").Read<IdValueRow>()).ToList();
			Check.That(
				() => allRows.Count == 2,
				() => r.GetResource("all-parts").Read<IdValueRow>().Count() == allRows.Count);
		}

		[Fact]
		public void resource_without_header() {
			var dp = new DataPackage();
			dp.AddResource(new DataPackageResourceOptions {
				Name = "stuff",
				HasHeaderRow = false,
			}, () => SequenceDataReader.Items(new { Id = 1, Value = "Stuff" }));

			var outputs = new Dictionary<string, MemoryStream>();
			dp.Save(path => outputs[path] = new MemoryStream());


			Check.That(() => Encoding.UTF8.GetString(outputs["stuff.csv"].ToArray()).TrimEnd() == "1;Stuff");
		}

		[Fact]
		public void update_schema_fields_on_read_when_not_set() {
			var dp = new DataPackage()
				.AddResource("my-resource", () => SequenceDataReader.Items(new { Id = 1, Value = "One" }))
				.Done();

			Check.Exception<InvalidOperationException>(() => dp.GetResource("my-resource").GetDescription());
		}

		[Fact]
		public void WithPrimaryKey_composite_key() {
			var dp = new DataPackage()
				.AddResource("my-resource", () => SequenceDataReader.Items(new { Id = 1, Value = "One" }))
				.WithPrimaryKey("Id", "Value");
		}

		[Fact]
		public void WithPrimaryKey_array_version() {
			var dp = new DataPackage()
				.AddResource("my-resource", () => SequenceDataReader.Items(new { Id = 1, Value = "One" }))
				.WithPrimaryKey(new[]{ "Id", "Value" });
		}

		[Fact]
		public void Save_normalizes_number_format() {
			var dp = new DataPackage();
			dp.AddResource("numbers", () => SequenceDataReader.Items(new { Value = 1.0 }));

			var xs = dp.Serialize(CultureInfo.GetCultureInfo("se-SV"));
			Check.That(
				() => GetNumbersFormat(xs).NumberDecimalSeparator == ",",
				() => GetNumbersFormat(xs.Serialize(CultureInfo.InvariantCulture)).NumberDecimalSeparator == ".",
				() => GetNumbersFormat(xs.Serialize(null)).NumberDecimalSeparator == ",");
		}

		[Fact]
		public void datetime_types() { 
			var dp = new DataPackage()
				.AddResource("dates-and-time", () => new[]{
					new {
						datetime = DateTime.Now,
						date = (DataPackageDate)DateTime.Now,
						time = DateTime.Now.TimeOfDay,
					}, 
				})
				.Serialize();

			var r = dp.GetResource("dates-and-time");
			Check.That(
				() => r.Schema.Fields[0].Name == "datetime",
				() => r.Schema.Fields[0].Type == "datetime",
				() => r.Schema.Fields[1].Name == "date",
				() => r.Schema.Fields[1].Type == "date",
				() => r.Schema.Fields[2].Name == "time",
				() => r.Schema.Fields[2].Type == "time");
		}

		[Fact]
		public void datetime_roundtrip() {
			var timestamp = DateTime.Now;
			//clamp to seconds precision
			timestamp = new DateTime(timestamp.Year, timestamp.Month, timestamp.Day, timestamp.Hour, timestamp.Minute, timestamp.Second, timestamp.Kind );
			var dp = new DataPackage()
				.AddResource("dates-and-time", () => new[]{
					new {
						datetime = timestamp,
						date = (DataPackageDate)timestamp,
						dpDate = timestamp,
					},
				})
				.Serialize();

			var r = dp.GetResource("dates-and-time").Read<DateTimeFormatRow>().Single();
			Check.That(
				() => r.datetime == timestamp,
				() => r.date == timestamp.Date,
				() => (DateTime)r.dpDate == timestamp.Date);
		}

		[Fact]
		public void time() {
			var t = DateTime.Now;
			var dp = new DataPackage()
				.AddResource("times", () => new[]{
					new {
						time = t.TimeOfDay,
					},
				})
				.Serialize();

			var rows = dp.GetResource("times").Read<DateTimeFormatRow>().Single();
			var reader = dp.GetResource("times").Read();
			reader.Read();
			Check.That(
				() => rows.time == new TimeSpan(t.TimeOfDay.Hours, t.TimeOfDay.Minutes, t.TimeOfDay.Seconds),
				() => reader.GetValue(0) == (object)rows.time,
				() => reader.GetString(0) == t.ToString("HH:mm:ss"));
		}

		[Fact]
		public void date_roundtrippy() {
			var today = DateTime.Today;
			var utc = today.ToUniversalTime().Date;

			var dp = new DataPackage()
				.AddResource("dates-are-dates", () => SequenceDataReader.Items(
					new {
						Today = (DataPackageDate)today,
						UtcToday = (DataPackageDate)utc,
					}
			))
			.Serialize()
			.Serialize();

			var r = dp.Resources[0].Read();
			r.Read();
			Check.That(
				() => today.Date != utc.Date,
				() => r["Today"] == (object)today,
				() => r["UtcToday"] == (object)utc);
		}

		[Fact]
		public void bytes_are_binary() {
			var bytes = Encoding.UTF8.GetBytes("Hello World!");
			var dp = new DataPackage()
				.AddResource("bytes", new[] {
					new { bytes, }
				}).Serialize();

			var r = dp.Resources.Single();
			var rows = r.Read();
			rows.Read();
			Check.That(
				() => r.Schema.Fields[0].Name == "bytes",
				() => r.Schema.Fields[0].Type == "string",
				() => r.Schema.Fields[0].Format == "binary",
				() => rows.GetFieldType(0) == typeof(byte[]),
				() => ((byte[])rows["bytes"]).SequenceEqual(bytes));
		}

		[Fact]
		public void custom_resource_delimiter() {
			var dp = new DataPackage()
				.AddResource("stuff", () => new[] { new { Id = 1, Message = "Hello World." } })
				.WithDelimiter("|");

			var bytes = new MemoryStream();
			dp.Save(x => x switch {
				"datapackage.json" => bytes,
				_ => Stream.Null,
			});

			var description = JsonConvert.DeserializeObject<DataPackageDescription>(Encoding.UTF8.GetString(bytes.ToArray()));
			var dp2 = dp.Serialize();
			Check.That(
				() => description.Resources[0].Dialect.Delimiter == "|",
				() => (dp2.Resources[0] as CsvDataResource).Delimiter == "|");
		}

		[Fact]
		public void transform_keeps_delimiter() {
			var dp = new DataPackage()
				.AddResource("stuff", () => new[] { new { Id = 1, Message = "Hello World." } })
				.WithDelimiter("|")
				.Done();

			dp.TransformResource("stuff", xs => xs.Transform("Id", x => x["Id"].ToString()));

			var bytes = new MemoryStream();
			dp.Save(x => x switch {
				"datapackage.json" => bytes,
				_ => Stream.Null,
			});

			var description = JsonConvert.DeserializeObject<DataPackageDescription>(Encoding.UTF8.GetString(bytes.ToArray()));
			var dp2 = dp.Serialize();
			Check.That(
				() => description.Resources[0].Dialect.Delimiter == "|",
				() => (dp2.Resources[0] as CsvDataResource).Delimiter == "|");

		}


		class DateTimeFormatRow
		{
			#pragma warning disable CS0649//never assigned.
			public DateTime datetime;
			public DateTime date;
			public DataPackageDate dpDate;
			public TimeSpan time;
			#pragma warning restore CS0649
		}

		NumberFormatInfo GetNumbersFormat(DataPackage data) => data.GetResource("numbers").Schema.Fields.Single().GetNumberFormat();
	}
}
