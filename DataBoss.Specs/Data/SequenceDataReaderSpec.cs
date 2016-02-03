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
		}

		public void can_map_field_by_name()
		{
			var reader = SequenceDataReader.For(new[] { new DataThingy { TheField = 42 } });
			reader.Map("TheField");

			Check.That(
				() => reader.GetOrdinal("TheField") == 0,
				() => reader.GetName(0) == "TheField");
			Check.That(() => reader.Read());
			Check.That(() => (int)reader[0] == 42);
		}
	}
}
