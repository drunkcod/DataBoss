using Cone;
using DataBoss.Data;
using System.Collections.Generic;
using System.Data;

namespace DataBoss.Specs
{
	[Describe(typeof(ConverterFactory))]
	public class ConverterFactorySpec
	{
		public void reuse_converter_for_matching_field_map() {
			var factory = new ConverterFactory(new ConverterCollection());

			var map = new FieldMap();
			map.Add("key", 0, typeof(int), true);
			map.Add("value", 1, typeof(string), true);

			var reader0 = SequenceDataReader.Create(new[]{ new { key = 0, value = "0"}}, x => x.MapAll());
			var reader1 = SequenceDataReader.Create(new[] { 1 }, x => {
				x.Map("key", item => item);
				x.Map("value", item => item.ToString());
			});

			Check.That(() => factory.GetConverter<IDataReader, KeyValuePair<int, string>>(reader0).Compiled == factory.GetConverter<IDataReader, KeyValuePair<int, string>>(reader1).Compiled);
		}
	}
}
