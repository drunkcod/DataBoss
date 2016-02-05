using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
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
			var reader = SequenceDataReader.For(new[] { new DataThingy { TheField = 42 } });
			reader.Map("TheField");

			Check.That(
				() => reader.GetOrdinal("TheField") == 0,
				() => reader.GetName(0) == "TheField");
			Check.That(() => reader.Read());
			Check.That(() => (int)reader[0] == 42);
		}

		public void can_map_property_by_name() {
			var reader = SequenceDataReader.For(new[] { new DataThingy { TheProp = "Hello World" } });
			reader.Map("TheProp");

			Check.That(() => reader.Read());
			Check.That(() => (string)reader[0] == "Hello World");
		}

		public void reports_unknown_member_in_sane_way() {
			var reader = SequenceDataReader.For(new[] { new DataThingy { TheProp = "Hello World" } });
			Check.Exception<InvalidOperationException>(() => reader.Map("NoSuchProp"));

		}

		[Context("Create")]
		public class SequenceDataReader_Create
		{
			public void maps_given_members_by_name() {
				var reader = SequenceDataReader.Create(new[] 
				{
					new DataThingy
					{
						TheField = 42,
						TheProp = "FooBar"
					}
				}, "TheProp", "TheField");

				Check.That(() => reader.FieldCount == 2);
				Check.That(
					() => reader.GetOrdinal("TheProp") == 0,
					() => reader.GetOrdinal("TheField") == 1
				);
			}

			public void maps_given_members() {
				var reader = SequenceDataReader.Create(new[] 
				{
					new DataThingy
					{
						TheField = 42,
						TheProp = "FooBar"
					}
				}, typeof(DataThingy).GetProperty("TheProp"), typeof(DataThingy).GetField("TheField"));

				Check.That(() => reader.FieldCount == 2);
				Check.That(
					() => reader.GetOrdinal("TheProp") == 0,
					() => reader.GetOrdinal("TheField") == 1
				);
			}
		}
	}
}
