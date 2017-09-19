using System;
using System.Linq;
using Cone;
using DataBoss.Data;

namespace DataBoss.Specs.Data
{
	[Describe(typeof(SequenceDataReader))]
	public class SequenceDataReaderSpec
	{
		class DataThingy
		{
			public int TheField;
			public string TheProp { get; set; }
		}

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

		public void can_map_property_by_name() {
			var reader = SequenceDataReader.Create(
				new[] { new DataThingy { TheProp = "Hello World" } },
				fields => fields.Map("TheProp")
			);

			Check.That(() => reader.Read());
			Check.That(() => (string)reader[0] == "Hello World");
		}

		public void reports_unknown_member_in_sane_way() {
			Check.Exception<InvalidOperationException>(() =>
				SequenceDataReader.Create(
					new[] { new DataThingy { TheProp = "Hello World" } },
					fields => fields.Map("NoSuchProp"))
				);
		}

		[Row("TheField" ,true)
		,Row("TheProp", true)
		,Row("GetHashCode", false)]
		public void map_by_member(string memberName, bool canMap) {
			var member = typeof(DataThingy).GetMember(memberName).Single();
			var fields = new FieldMapping<DataThingy>();
			if(canMap) 
				Check.That(() => fields.Map(member) == 0);
			else Check.Exception<ArgumentException>(() => fields.Map(member));
		}

		[Context("Create")]
		public class SequenceDataReader_Create
		{
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
		}

		class MyRow<T>
		{
			public T Value;
		}

		public void roundtrip_nullable_field() {
			var items = new[] { new MyRow<int?> { Value = 42 } };	
			var reader = SequenceDataReader.Create(items, x => x.MapAll());
	
			Check.With(() => ObjectReader.For(reader).Read<MyRow<int?>>().ToList())
				.That(rows => rows[0].Value == items[0].Value);
		}

		public void roundtrip_nullable_field_null() {
			var items = new[] { new MyRow<float?> { Value = null } };	
			var reader = SequenceDataReader.Create(items, x => x.MapAll());
	
			Check.With(() => ObjectReader.For(reader).Read<MyRow<float?>>().ToList())
				.That(rows => rows[0].Value == null);
		}
	}
}
