using System;
using System.Globalization;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using CheckThat;
using DataBoss.Data;
using DataBoss.Data.Common;
using DataBoss.DataPackage.Schema;
using DataBoss.DataPackage.Types;
using Xunit;

namespace DataBoss.DataPackage
{
	public partial class DataPackageSpec
	{
		struct IdValueRow
		{
			#pragma warning disable CS0649//never assigned.
			public int Id;
			public string Value;
			#pragma warning restore CS0649
		}

		[Fact]
		public void getting_unknown_resource_is_an_InvalidOperation() {
			var dp = new DataPackage();

			var e = Check.Exception<InvalidOperationException>(() => dp.GetResource("no-such-resource"));
			Check.That(() => e.Message.Contains("no-such-resource"));
		}

		[Fact]
		public void cant_add_duplicate_resource() {
			var dp = new DataPackage();

			dp.AddResource(x => x.WithName("stuff").WithData(new[] { new { Value = 1 } }));
			Check.Exception<InvalidOperationException>(
				() => dp.AddResource(x => x.WithName("stuff").WithData(new[] { new { Value = 2 } })));
		}

		[Fact]
		public void AddResource_with_non_default_path() {
			var dp = new DataPackage()
				.AddResource(new CsvResourceOptions {
					Name = "my-resource",
					Path = "some/path.csv",
				}, () => SequenceDataReader.Items(new { Id = 1, Value = "One" }));

			var r = dp.Serialize();

			Check.That(() => r.Resources[0].ResourcePath.Single() == "some/path.csv");
		}

		[Fact]
		public void resource_alias() {
			var dp = new DataPackage();
			dp.AddResource(new CsvResourceOptions {
				Name = "1",
				Path = "parts/1.csv",
			}, () => SequenceDataReader.Items(new { Id = 1, Value = "One" }));

			dp.AddResource(new CsvResourceOptions {
				Name = "also-1",
				Path = "parts/1.csv"
			});

			var r = dp.Serialize();;

			Check.That(
				() => r.GetResource("also-1").Read<IdValueRow>().SequenceEqual(r.GetResource("1").Read<IdValueRow>()));
		}

		[Fact]
		public void multi_part_resource() {
			var dp = new DataPackage();
			dp.AddResource(new CsvResourceOptions {
				Name = "1",
				Path = "parts/1.csv",
			}, () => SequenceDataReader.Items(new { Id = 1, Value = "One" }));
			
			dp.AddResource(new CsvResourceOptions {
				Name = "2",
				Path = "parts/2.csv",
				HasHeaderRow = false,
			}, () => SequenceDataReader.Items(new { Id = 2, Value = "Two" }));

			dp.AddResource(new CsvResourceOptions {
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
			dp.AddResource(new CsvResourceOptions {
				Name = "stuff",
				HasHeaderRow = false,
			}, () => SequenceDataReader.Items(new { Id = 1, Value = "Stuff" }));


			var store = new InMemoryDataPackageStore();
			store.Save(dp);

			Check.That(() => Encoding.UTF8.GetString(store.ReadAllBytes("stuff.csv")).TrimEnd() == "1;Stuff");
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
				.AddResource(x => x
					.WithName("dates-and-time")
					.WithData(new [] {
						new {
							datetime = DateTime.Now,
							date = (DataPackageDate)DateTime.Now,
							time = DateTime.Now.TimeOfDay,
						}, }))
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
				.AddResource(x => x
					.WithName("dates-and-time")
					.WithData(new[]{
						new {
							datetime = timestamp,
							date = (DataPackageDate)timestamp,
							dpDate = timestamp,
						},
					}))
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
				.AddResource(x => x
					.WithName("times")
					.WithData(new [] {
						new { time = t.TimeOfDay,
					}, }))
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
				.AddResource(x => x
					.WithName("bytes")
					.WithData(new[] { new { bytes, } }))
				.Serialize();

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
		public void a_character_is_a_string_of_length_one() {
			var dp = new DataPackage()
				.AddResource(x => x
					.WithName("chars")
					.WithData(new[] { new { Value = '☺' } }))
				.Serialize();

			var r = dp.Resources.Single();
			var readerSchema = r.Read().GetDataReaderSchemaTable();
			var rows = r.Read<MyRow<char>>().ToList();
			Check.That(
				() => r.Schema.Fields[0].Name == "Value",
				() => r.Schema.Fields[0].Type == "string",
				() => r.Schema.Fields[0].Constraints.Value.MaxLength == 1,
				() => rows[0].Value == '☺',
				() => readerSchema[0].ColumnName == "Value",
				() => readerSchema[0].AllowDBNull == false,
				() => readerSchema[0].ColumnSize == 1);
		}

		[Fact]
		public void guid_is_a_uuid_string() {
			var value = Guid.NewGuid();
			var dp = new DataPackage()
				.AddResource(x => x
					.WithName("uuids")
					.WithData(new [] { new { Value = value } }))
				.Serialize();

			var r = dp.Resources.Single();
			var readerSchema = r.Read().GetDataReaderSchemaTable();
			var rows = r.Read<MyRow<Guid>>().ToList();
			Check.That(
				() => r.Schema.Fields[0].Name == "Value",
				() => r.Schema.Fields[0].Type == "string",
				() => r.Schema.Fields[0].Format == "uuid",
				() => rows[0].Value == value);
		}

		class MyRow<T>
		{
			#pragma warning disable CS0649//never assigned.
			public T Value;
			#pragma warning restore CS0649
		}

		[Fact]
		public void custom_resource_delimiter() {
			var dp = new DataPackage()
				.AddResource(x => x
					.WithName("stuff")
					.WithData(new[] { new { Id = 1, Message = "Hello World." } })
					.WithDelimiter("|"))
				.Done();

			var store = new InMemoryDataPackageStore();
			store.Save(dp);

			var description = store.GetDataPackageDescription();
			var dp2 = store.Load();
			Check.That(
				() => description.Resources[0].Dialect.Delimiter == "|",
				() => (dp2.Resources[0] as CsvDataResource).Delimiter == "|");
		}

		[Fact]
		public void transform_keeps_delimiter() {
			var dp = new DataPackage()
				.AddResource(x => x
					.WithName("stuff")
					.WithData(new[] { new { Id = 1, Message = "Hello World." } })
					.WithDelimiter("|"))
				.Done();

			dp.TransformResource("stuff", xs => xs.Transform("Id", x => x.Source["Id"].ToString()));

			var store = new InMemoryDataPackageStore();
			store.Save(dp);

			var description = store.GetDataPackageDescription();
			var dp2 = store.Load();
			Check.That(
				() => description.Resources[0].Dialect.Delimiter == "|",
				() => (dp2.Resources[0] as CsvDataResource).Delimiter == "|");
		}

		[Fact]
		public void remove_resource() {
			var dp = new DataPackage();

			dp.AddResource(x => x.WithName("resource-1").WithData(new[] { new { Id = 1 } }));
			dp.AddResource(x => x.WithName("resource-2").WithData(new[] { new { Id = 2 } }));

			dp.RemoveResource("resource-1");
			Check.That(
				() => dp.Resources.Any(x => x.Name == "resource-1") == false);
		}

		[Fact]
		public void remove_resource_serialized() {
			var dp = new DataPackage();

			dp.AddResource(x => x.WithName("resource-1").WithData(new[] { new { Id = 1 } }));
			dp.AddResource(x => x.WithName("resource-2").WithData(new[] { new { Id = 2 } }));

			var loaded = dp.Serialize();
			loaded.RemoveResource("resource-1");
			Check.That(
				() => loaded.Resources.Any(x => x.Name == "resource-1") == false);
		}

		[Fact]
		public void cant_remove_referenced_resource() {
			var dp = new DataPackage();

			dp.AddResource(x => x.WithName("resource-1").WithData(new[] { new { Id = 1 } }));
			dp.AddResource(x => x
				.WithName("resource-2")
				.WithData(new[] { new { Id = 1 } })
				.WithForeignKey("Id", new DataPackageKeyReference("resource-1", "Id")));

			Check.Exception<InvalidOperationException>(
				() => dp.RemoveResource("resource-1"));
		}

		[Fact]
		public void remove_resource_drop_constraints() {
			var dp = new DataPackage();

			dp.AddResource(x => x.WithName("resource-1").WithData(new[] { new { Id = 1 } }));
			dp.AddResource(x => x
				.WithName("resource-2")
				.WithData(new[] { new { Id = 1 } })
				.WithForeignKey("Id", new DataPackageKeyReference("resource-1", "Id")));

			dp.RemoveResource("resource-1", ConstraintsBehavior.Drop, out var _);
			Check.That(
				() => dp.GetResource("resource-2").Schema.ForeignKeys.Any() == false);
		}

		[Fact]
		public void AddOrUpdateResource_must_supply_data() {
			var dp = new DataPackage();

			Action<CsvResourceBuilder> doNothing = x => { };
			Check.Exception<InvalidOperationException>(() =>
				dp.AddOrUpdateResource("stuff", doNothing, x => x));
		}

		[Fact]
		public void AddOrUpdateResource_update_updates() {
			var dp = new DataPackage();
			Action<CsvResourceBuilder> create = x => x.WithData(() => SequenceDataReader.Items(new { Id = 1 }));
			dp.AddOrUpdateResource("stuff", create, x => x);
			dp.AddOrUpdateResource("stuff", create, x => x.WithData(() => x.Read().Concat(new[] { new { Id = 2 } })));

			var rows = dp.Serialize().GetResource("stuff").Read<IdRow<int>>().ToList();
			Check.That(() => rows.Count == 2);
		}

		[Fact]
		public void csv_resource_defaults() {
			var dp = new DataPackage();

			dp.AddResource(x => x
				.WithName("data")
				.WithData(new[] { new { Value = 1 } }));

			var csv = (CsvDataResource)dp.GetResource("data");
			var defaultDialect = CsvDialectDescription.GetDefaultDialect();
			Check.That(
				() => csv.HasHeaderRow == defaultDialect.HasHeaderRow,
				() => csv.Delimiter == null);
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

	public partial class DataPackageSpec
	{
		public class ResourceCompressionSpec
		{
			[Fact]
			public void normalize_gzip_resource_names() {
				var store = new InMemoryDataPackageStore();

				var dp = new DataPackage();
				dp.AddResource(x => x.WithName("data").WithData(new[] { new { Value = 1 } }));
				dp.Save(store.OpenWrite, new DataPackageSaveOptions {
					ResourceCompression = ResourceCompression.GZip,
				});

				var loaded = store.Load();
				var store2 = new InMemoryDataPackageStore();
				loaded.Save(store2.OpenWrite);
				var load2 = store2.Load();
				Check.That(() => load2.GetResource("data").ResourcePath == "data.csv");
			}

			[Fact]
			public void zip_source_normalize_gzip_resource_names() {
				var bytes = new MemoryStream();

				var dp = new DataPackage();
				dp.AddResource(x => x.WithName("data").WithData(new[] { new { Value = 1 } }));
				dp.SaveZip(bytes, new DataPackageSaveOptions {
					ResourceCompression = ResourceCompression.GZip,
				});

				var loaded = DataPackage.LoadZip(() => new MemoryStream(bytes.ToArray()));
				Check.That(() => loaded.GetResource("data").ResourcePath == "data.csv");
			}
		}
	}

	public partial class DataPackageSpec
	{
		public class ZipResourceCompression
		{
			InMemoryDataPackageStore store = new();

			public ZipResourceCompression() {
				var dp = new DataPackage();
				dp.AddResource(x => x.WithName("data").WithData(new[] { new { Value = 1 } }));
				dp.Save(store.OpenWrite, new DataPackageSaveOptions {
					ResourceCompression = ResourceCompression.Zip,
				});
			}

			[Fact]
			public void contains_data_zip() => Check.That(() => store.Contains("data.zip"));

			[Fact]
			public void zip_contains_csv() {
				var zip = new ZipArchive(store.OpenRead("data.zip"), ZipArchiveMode.Read);

				Check.That(
					() => zip.Entries.Count == 1,
					() => zip.Entries[0].Name == "data.csv");
			}

			[Fact]
			public void data_resource() {
				var dp = store.Load();
				Check.That(() => dp.GetResource("data").Read<ValueRow>().Single().Value == 1);
			}

			class ValueRow { public int Value; }
		}
	}
}
