using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using CheckThat;
using DataBoss.Data.Common;
using DataBoss.DataPackage;
using Xunit;

namespace DataBoss.Data
{
	public class ConcatDataReaderTests
	{
		[Fact]
		public void rows_are_in_reader_order() {
			var r0 = SequenceDataReader.Items(new { Reader = 0, Item = 1 });
			var r1 = SequenceDataReader.Items(new { Reader = 1, Item = 1 }, new { Reader = 1, Item = 2 });

			var xs = r0.Concat(r1).Read<ReaderRow>().ToList();
			Check.That(() => xs.Count == 3);
		}

		[Fact]
		public void mismatched_column_names() {
			var r0 = SequenceDataReader.Items(new { Id = 1 });
			var r1 = SequenceDataReader.Items(new { Value = 1 });

			Check.Exception<InvalidOperationException>(() => r0.Concat(r1));
		}

		[Fact]
		public void mismatched_column_types() {
			var r0 = SequenceDataReader.Items(new { Id = 1 });
			var r1 = SequenceDataReader.Items(new { Id = (float)1 });

			Check.Exception<InvalidOperationException>(() => r0.Concat(r1));
		}

		[Fact]
		public void mismatched_column_nullability() {
			var r0 = SequenceDataReader.Items(new { Id = 1 });
			var r1 = SequenceDataReader.Items(new { Id = (int?)1 });

			Check.Exception<InvalidOperationException>(() => r0.Concat(r1));
		}

		[Fact]
		public void allow_non_nullable_column_as_nullable() {
			var r0 = SequenceDataReader.Items(new { Id = (int?)1 });
			var r1 = SequenceDataReader.Items(new { Id = 2 });

			var row = r0.Concat(r1).Read<IdRow<int>>().ToList();
			Check.That(
				() => row.Count == 2,
				() => row[0].Id == 1);
		}

		[Fact]
		public void hide_provider_specific_value_if_different() {
			var r0 = GetCsvDataReader(new { Id = 1 });
			var r1 = SequenceDataReader.Items(new { Id = 2 });

			var all = r0.Concat(r1);
			var schema = all.GetDataReaderSchemaTable();
			Check.That(() => schema[0].ProviderSpecificDataType == null);
		}

		[Fact]
		public void keep_provider_specific_value_if_same() {
			var r0 = GetCsvDataReader(new { Id = 0 });
			var r1 = GetCsvDataReader(new { Id = 1 });

			var all = r0.Concat(r1);
			var schema = all.GetDataReaderSchemaTable();
			Check.That(() => schema[0].ProviderSpecificDataType == typeof(CsvInteger));
		}

		[Fact]
		public void can_read_provider_specific_type() {
			var r0 = GetCsvDataReader(new { Id = 1 });
			var r1 = GetCsvDataReader(new { Id = (long)int.MaxValue + 1 });

			var all = r0.Concat(r1);
			var schema = all.GetDataReaderSchemaTable();
			var rows = all.Read<IdRow<long>>().ToList();
			Check.That(
				() => rows[0].Id == 1,
				() => rows[1].Id == (long)int.MaxValue + 1);
		}

		static IDataReader GetCsvDataReader<T>(params T[] items) =>
			new DataPackage.DataPackage().AddResource("items", items).Serialize().GetResource("items").Read();

		[Fact]
		public void Dispose_disposes_all_readers() {
			var r0 = new DataReaderDecorator(SequenceDataReader.Items(new { Id = 1 }));
			var r1 = new DataReaderDecorator(SequenceDataReader.Items(new { Id = 2 }));

			var disposed = new List<string>();

			r0.Disposed += () => disposed.Add("r0");
			r1.Disposed += () => disposed.Add("r1");

			r0.Concat(r1).Dispose();
			Check.That(
				() => disposed.Count == 2,
				() => disposed[0] == "r0", 
				() => disposed[1] == "r1");
		}

		[Fact]
		public void dispose_reader_when_rows_consumed() {
			var r0 = new DataReaderDecorator(SequenceDataReader.Items(new { Id = 1 }));
			var r1 = new DataReaderDecorator(SequenceDataReader.Items(new { Id = 2 }));

			var disposed = new List<string>();

			r0.Disposed += () => disposed.Add("r0");
			r1.Disposed += () => disposed.Add("r1");

			var rows = r0.Concat(r1);
			rows.Read();
			rows.Read();
			Check.That(
				() => disposed.Count == 1,
				() => disposed[0] == "r0");

			rows.Dispose();
			Check.That(
				() => disposed.Count == 2,
				() => disposed[1] == "r1");
		}

		[Fact]
		public void combines_multiple_concats() {
			var r0 = SequenceDataReader.Items(new { Id = 1 });
			var r1 = SequenceDataReader.Items(new { Id = 2 });
			var r2 = SequenceDataReader.Items(new { Id = 3 });

			var all = r0.Concat(r1).Concat(r2);

			Check.That(() => ((ConcatDataReader)all).ReaderCount == 3);
		}

		class ReaderRow { public int Reader; public int Item; };
	}
}
