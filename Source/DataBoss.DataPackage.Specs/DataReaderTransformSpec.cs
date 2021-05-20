using System;
using System.Collections.Generic;
using CheckThat;
using DataBoss.Data;
using Xunit;

namespace DataBoss.DataPackage
{
	public class DataReaderTransformSpec
	{
		[Fact]
		public void Transform_value_keeps_source_field_nullability() {
			var rows = SequenceDataReader.Items(
				new { NullableField = (DateTime?)DateTime.Now },
				new { NullableField = (DateTime?)null });

			var xform = rows.WithTransform(x => x.Transform("NullableField", (DateTime x) => x));
			Check.That(() => xform.GetDataReaderSchemaTable()[0].AllowDBNull == true);
		}

		[Fact]
		public void Transform_record_nullable_result() {
			var rows = SequenceDataReader.Items(
				new { Value = 1 });

			var xform = rows.WithTransform(x => {
				var value = x.GetOrdinal("Value");
				x.Transform("Value", x => (int?)x.GetInt32(value));
			});
			Check.That(
				() => xform.GetFieldType(0) == typeof(int),
				() => xform.GetDataReaderSchemaTable()[0].AllowDBNull == true);
		}

		[Fact]
		public void Transform_to_non_nullable() {
			var rows = SequenceDataReader.Items(
				new { Value = (int?)1 });

			var xform = rows.WithTransform(x => x.Transform("Value", (int? x) => x ?? -1));
			Check.That(
				() => xform.GetFieldType(0) == typeof(int),
				() => xform.GetDataReaderSchemaTable()[0].AllowDBNull == false);
		}

		[Fact]
		public void Transform_to_nullable() {
			var rows = SequenceDataReader.Items(
				new { Value = 1 });

			var xform = rows.WithTransform(x => x.Transform("Value", (int x) => (int?)x));
			xform.Read();
			Check.That(
				() => xform.GetFieldType(0) == typeof(int),
				() => xform.GetDataReaderSchemaTable()[0].AllowDBNull,
				() => (int)xform.GetValue(0) == 1,
				() => xform.GetInt32(0) == 1);
		}

		[Fact]
		public void Transform_to_nullable_null() {
			var rows = SequenceDataReader.Items(
				new { Value = 1 });

			var xform = rows.WithTransform(x => x.Transform("Value", (int x) => (int?)null));
			xform.Read();
			Check.That(
				() => xform.GetFieldType(0) == typeof(int),
				() => xform.GetDataReaderSchemaTable()[0].AllowDBNull,
				() => xform.IsDBNull(0),
				() => xform.GetValue(0) == DBNull.Value);
		}

		[Fact]
		public void Add_non_nullable() {
			var rows = SequenceDataReader.Items(
				new { Value = 1 });

			var xform = rows.WithTransform(x => x.Add("PI", _ => 3.14));
			xform.Read();
			Check.That(
				() => xform.GetFieldType(1) == typeof(double),
				() => xform.GetDataReaderSchemaTable()[1].AllowDBNull == false,
				() => xform.IsDBNull(1) == false);
		}

		[Fact]
		public void Add_nullable() {
			var rows = SequenceDataReader.Items(
				new { Value = 1 },
				new { Value = 2 });

			var xform = rows.WithTransform(x => x.Add("NullIfOdd", r => {
				var value = r.GetInt32(0);
				return value % 2 != 0 ? (float?)null : value;
			}));
			var nullIfEven = new List<(bool IsDBNull,object Value)>();
			while (xform.Read())
				nullIfEven.Add((xform.IsDBNull(1), xform.GetValue(1)));
			Check.That(
				() => xform.GetFieldType(1) == typeof(float),
				() => xform.GetDataReaderSchemaTable()[1].AllowDBNull == true,
				() => nullIfEven[0].IsDBNull == true,
				() => nullIfEven[0].Value == DBNull.Value,
				() => nullIfEven[1].IsDBNull == false,
				() => (float)nullIfEven[1].Value == 2.0);
		}

		[Fact]
		public void synthetic_columns_only_evaluated_once_per_row() {
			var rows = SequenceDataReader.Items(
				new { Value = 1 });

			var n = 0;
			var xform = rows.WithTransform(x => x.Add("N", _ => (int?)++n));
			xform.Read();
			xform.IsDBNull(1);
			xform.GetInt32(1);
			xform.GetValue(1);
			Check.That(() => n == 1);
		}

		[Fact]
		public void rename() {
			var rows = SequenceDataReader.Items(new { Id = 1 });

			var xform = rows.WithTransform(x => x.Rename("Id", "Value"));
			Check.That(() => xform.GetOrdinal("Value") == 0);
		}

		[Fact]
		public void null_string_is_null() =>
			Check.That(() => DataReaderTransform.FieldInfo<string>.IsNull(null));

		[Fact]
		public void Add_at() {
			var rows = SequenceDataReader.Items(new { Value = "Hello" });

			var xform = rows.WithTransform(x => x.Add(0, "Id", x => 1));

			Check.That(
				() => xform.GetOrdinal("Id") == 0,
				() => xform.GetOrdinal("Value") == 1,
				() => xform.GetName(0) == "Id",
				() => xform.GetName(1) == "Value",
				() => xform.GetFieldType(0) == typeof(int),
				() => xform.GetFieldType(1) == typeof(string));
		}

		[Fact]
		public void Transform_handles_changed_column_order() {
			var rows = SequenceDataReader.Items(new { Foo = "Hello", Bar = "World" });
			var xform = rows.WithTransform(x => {
				x.Add(0, "Id", x => 1); //displace columns
				x.Transform("Foo", (string value) => value.ToUpper());
				x.Transform(2, (string value) => value.ToLower());
			});

			xform.Read();
			Check.That(
				() => xform.GetOrdinal("Foo") == 1,
				() => xform.GetString(1) == "HELLO",
				() => xform.GetOrdinal("Bar") == 2,
				() => xform.GetString(2) == "world");
		}

		[Fact]
		public void Transform_typed_record() {
			var item = new { Value = 1 };
			var rows = SequenceDataReader.Items(item);

			var xform = rows.WithTransform(x => x.Add(0, "Text", (ValueRow x) => x.Value.ToString()));
			xform.Read();
			Check.That(
				() => xform.GetOrdinal("Text") == 0,
				() => xform.GetOrdinal("Value") == 1,
				() => xform.GetName(0) == "Text",
				() => xform.GetName(1) == "Value",
				() => xform.GetFieldType(0) == typeof(string),
				() => xform.GetFieldType(1) == typeof(int),
				() => xform.GetString(0) == item.Value.ToString());
		}

		[Fact]
		public void Transform_typed_record_avoids_sels_recursion() {
			var item = new { Value = 1 };
			var rows = SequenceDataReader.Items(item);

			var xform = rows.WithTransform(x => x.Add("Text", (ValueTextRow x) => x.Value.ToString()));
			xform.Read();
			Check.That(
				() => xform.GetOrdinal("Text") == 1,
				() => xform.GetString(1) == item.Value.ToString());
		}

		struct ValueRow { public int Value; }
		struct ValueTextRow { public int Value; public string Text; };
	}
}
