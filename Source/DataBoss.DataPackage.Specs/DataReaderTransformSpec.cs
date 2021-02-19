using System;
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
				new { Value = 1 });

			var xform = rows.WithTransform(x => x.Add("Null", _ => (float?)null));
			xform.Read();
			Check.That(
				() => xform.GetFieldType(1) == typeof(float),
				() => xform.GetDataReaderSchemaTable()[1].AllowDBNull == true,
				() => xform.IsDBNull(1) == true,
				() => xform.GetValue(1) == DBNull.Value);
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

	}
}
