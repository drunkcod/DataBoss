using System;
using System.Collections.Generic;
using System.Linq;
using CheckThat;
using Xunit;

namespace DataBoss.Data
{
	public class SequenceDataReaderSpec
	{
		class DataThingy
		{
			public int TheField;
			public string TheProp { get; set; }
		}

		[Fact]
		public void can_map_field_by_name() {
			var reader = SequenceDataReader.Create(
				new[] { new DataThingy { TheField = 42 } },
				fields => fields.Map("TheField")
			);

			Check.That(
				() => reader.GetOrdinal("TheField") == 0,
				() => reader.GetName(0) == "TheField");
			Check.That(() => reader.Read());
			Check.That(() => (int)reader[0] == 42);
		}

		[Fact]
		public void can_map_property_by_name() {
			var reader = SequenceDataReader.Create(
				new[] { new DataThingy { TheProp = "Hello World" } },
				fields => fields.Map("TheProp")
			);

			Check.That(() => reader.Read());
			Check.That(() => (string)reader[0] == "Hello World");
		}

		[Fact]
		public void reports_unknown_member_in_sane_way() {
			Check.Exception<InvalidOperationException>(() =>
				SequenceDataReader.Create(
					new[] { new DataThingy { TheProp = "Hello World" } },
					fields => fields.Map("NoSuchProp"))
				);
		}

		[Theory]
		[InlineData("TheField" ,true)
		,InlineData("TheProp", true)
		,InlineData("GetHashCode", false)]
		public void map_by_member(string memberName, bool canMap) {
			var member = typeof(DataThingy).GetMember(memberName).Single();
			var fields = new FieldMapping<DataThingy>();
			if(canMap) 
				Check.That(() => fields.Map(member) == 0);
			else Check.Exception<ArgumentException>(() => fields.Map(member));
		}

		public class SequenceDataReader_Create
		{
			[Fact]
			public void maps_given_members_by_name() {
				var reader = SequenceDataReader.Create(new[] {
					new DataThingy {
						TheField = 42,
						TheProp = "FooBar"
					}
				}, "TheProp", "TheField");

				Check.That(() => reader.FieldCount == 2);
				Check.That(
					() => reader.GetOrdinal("TheProp") == 0,
					() => reader.GetOrdinal("TheField") == 1);
			}

			[Fact]
			public void maps_given_members() {
				var reader = SequenceDataReader.Create(new[] {
					new DataThingy {
						TheField = 42,
						TheProp = "FooBar"
					}
				}, typeof(DataThingy).GetProperty("TheProp"), typeof(DataThingy).GetField("TheField"));

				Check.That(() => reader.FieldCount == 2);
				Check.That(
					() => reader.GetOrdinal("TheProp") == 0,
					() => reader.GetOrdinal("TheField") == 1);
			}

			[Fact]
			public void map_all() {
				var reader = SequenceDataReader.Create(new[] {
					new DataThingy {
						TheField = 42,
						TheProp = "FooBar"
					}
				}, x => x.MapAll());

				Check.That(() => reader.FieldCount == 2);
				Check.That(
					() => reader.GetOrdinal("TheProp") != reader.GetOrdinal("TheField"));
			}

			[Fact]
			public void cant_create_reader_from_null_sequence() =>
				Check.Exception<ArgumentNullException>(() => SequenceDataReader.Create((IEnumerable<MyRow<int>>)null, x => x.MapAll()));
		}

		class MyRow<T>
		{
			public T Value;
		}

		[Fact]
		public void roundtrip_nullable_field() {
			var items = new[] { new MyRow<int?> { Value = 42 } };	
			var reader = SequenceDataReader.Create(items, x => x.MapAll());
	
			Check.With(() => ObjectReader.For(reader).Read<MyRow<int?>>().ToList())
				.That(rows => rows[0].Value == items[0].Value);
		}

		[Fact]
		public void roundtrip_nullable_field_null() {
			var items = new[] { new MyRow<float?> { Value = null } };	
			var reader = SequenceDataReader.Create(items, x => x.MapAll());
	
			Check.With(() => ObjectReader.For(reader).Read<MyRow<float?>>().ToList())
				.That(rows => rows[0].Value == null);
		}

		[Fact]
		public void treats_IdOf_as_int() {
			var items = new[] { new MyRow<IdOf<float>> { Value = (IdOf<float>)1 } };

			Check.With(() => SequenceDataReader.Create(items, x => x.MapAll()))
				.That(x => x.GetFieldType(0) == typeof(int));
		}
	}
}
