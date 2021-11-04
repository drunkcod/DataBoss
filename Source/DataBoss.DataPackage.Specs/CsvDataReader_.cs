using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using CheckThat;
using DataBoss.Data;
using DataBoss.Data.Common;
using DataBoss.Linq;
using Xunit;

namespace DataBoss.DataPackage
{
	public class CsvDataReader_
	{
		[Fact]
		public void field_nullability_defaults_to_true() {
			var csv = new CsvDataReader(
				new CsvHelper.CsvParser(TextReader.Null, CultureInfo.CurrentCulture),
				new TabularDataSchema {
					Fields = new List<TabularDataSchemaFieldDescription> {
						new TabularDataSchemaFieldDescription("boolean", "boolean"),
						new TabularDataSchemaFieldDescription("datetime", "datetime"),
						new TabularDataSchemaFieldDescription("integer", "integer"),
						new TabularDataSchemaFieldDescription("number", "number"),
					}
				}, hasHeaderRow: false);

			var schema = ObjectReader.For(csv.GetSchemaTable().CreateDataReader).Read<DataReaderSchemaRow>().ToList();
			Check.That(
				() => schema.Single(x => x.ColumnName == "boolean").AllowDBNull == true,
				() => schema.Single(x => x.ColumnName == "datetime").AllowDBNull == true,
				() => schema.Single(x => x.ColumnName == "integer").AllowDBNull == true,
				() => schema.Single(x => x.ColumnName == "number").AllowDBNull == true);
		}

		[Fact]
		public void required_field() {
			var csv = new CsvDataReader(
				new CsvHelper.CsvParser(TextReader.Null, CultureInfo.CurrentCulture),
				new TabularDataSchema {
					Fields = new List<TabularDataSchemaFieldDescription> {
						new TabularDataSchemaFieldDescription(
							"integer", 
							"integer", 
							constraints: new TabularDataSchemaFieldConstraints(required: true)),
					}
				}, hasHeaderRow: false);

			var schema = ObjectReader.For(csv.GetSchemaTable().CreateDataReader).Read<DataReaderSchemaRow>().ToList();
			Check.That(
				() => schema.Single(x => x.ColumnName == "integer").AllowDBNull == false);
		}

		[Fact]
		public void identity_column_is_required() {
			var csv = new CsvDataReader(
				new CsvHelper.CsvParser(TextReader.Null, CultureInfo.CurrentCulture),
				new TabularDataSchema {
					Fields = new List<TabularDataSchemaFieldDescription> {
						new TabularDataSchemaFieldDescription("id", "integer"),
					}, 
					PrimaryKey = new List<string>{ "id" },
				}, hasHeaderRow: false);

			var schema = ObjectReader.For(csv.GetSchemaTable().CreateDataReader).Read<DataReaderSchemaRow>().ToList();
			Check.That(() => schema.Single(x => x.ColumnName == "id").AllowDBNull == false);
		}

		[Fact]
		public void detect_missing_required_value() {
			var csv = new CsvDataReader(
				new CsvHelper.CsvParser(new StringReader("1,\n,\n"), CultureInfo.CurrentCulture),
				new TabularDataSchema {
					Fields = new List<TabularDataSchemaFieldDescription> {
						new TabularDataSchemaFieldDescription(
							"Id",
							"integer",
							constraints: new TabularDataSchemaFieldConstraints(required: true)),
					}
				}, hasHeaderRow: false);

			Check.Exception<InvalidOperationException>(() => ObjectReader.Read<IdRow<int>>(csv).ToList());
		}

		[Fact]
		public void support_varying_decimal_separator() {
			var csv = new CsvDataReader(
				new CsvHelper.CsvParser(new StringReader("3·1415"), CultureInfo.CurrentCulture),
				new TabularDataSchema {
					Fields = new List<TabularDataSchemaFieldDescription> {
						new TabularDataSchemaFieldDescription(
							"value",
							"number",
							decimalChar: "·"//interpunct, no-one uses that.
						),
					}
				}, hasHeaderRow: false);

			Check.With(() => ObjectReader.Read<ValueRow<double>>(csv).ToList()).That(				
				xs => xs.Count == 1,
				xs => xs[0].Value == 3.1415);
		}

		[Fact]
		public void gets_angry_when_read_not_called() {
			var dp = new DataPackage()
				.AddResource(x => x
					.WithName("numbers")
					.WithData(new[] { new { Value = 1 } }))
				.Serialize();

			var r = dp.GetResource("numbers").Read();

			Check.Exception<InvalidOperationException>(() => r.GetInt32(0));
		}

		[Fact]
		public void bytes() {
			var dp = new DataPackage();
			var bytes = Encoding.UTF8.GetBytes("Hello Byte World!");
			dp.AddResource(x => x.WithName("data").WithData(() => SequenceDataReader.Items(new ValueRow<byte[]> { Value = bytes })));

			var data = dp.Serialize().GetResource("data").Read();
			data.Read();

			var bigBuffer = new byte[bytes.Length * 2];
			var smallBuffer = new byte[bytes.Length / 2];
			Check.That(
				() => data is CsvDataReader,
				() => data.GetFieldType(0) == typeof(byte[]),
				() => data.GetBytes(0, 0, bigBuffer, 0, bigBuffer.Length) == bytes.Length,
				() => bigBuffer.Take(bytes.Length).SequenceEqual(bytes),
				() => data.GetBytes(0, 0, smallBuffer, 0, smallBuffer.Length) == smallBuffer.Length,
				() => bytes.Take(smallBuffer.Length).SequenceEqual(smallBuffer));
		}

		class ValueRow<T> { public T Value { get; set; } }
	}
}
