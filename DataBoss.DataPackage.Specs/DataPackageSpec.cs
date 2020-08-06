using System;
using System.Globalization;
using System.Linq;
using System.Text;
using Cone;
using DataBoss.Data;
using DataBoss.DataPackage.Types;
using Xunit;

namespace DataBoss.DataPackage.Specs
{
	public class DataPackageSpec
	{
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
				() => r.Schema.Fields[0].Type == "binary",
				() => ((byte[])rows["bytes"]).SequenceEqual(bytes));
		}

		class DateTimeFormatRow
		{
			public DateTime datetime;
			public DateTime date;
			public DataPackageDate dpDate;
			public TimeSpan time;
		}

		NumberFormatInfo GetNumbersFormat(DataPackage data) => data.GetResource("numbers").Schema.Fields.Single().GetNumberFormat();
	}
}
