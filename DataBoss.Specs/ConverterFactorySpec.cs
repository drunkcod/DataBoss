using Cone;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataBoss.Specs
{
	[Describe(typeof(ConverterFactory))]
	public class ConverterFactorySpec
	{
		public void reuse_converter_for_matching_field_map() {
			var factory = new ConverterFactory(typeof(IDataReader), new ConverterCollection());

			var map = new FieldMap();
			map.Add("key", 0, typeof(int));
			map.Add("value", 1, typeof(string));

			var targetType = typeof(KeyValuePair<int, string>);
			Check.That(() => factory.Converter(map, targetType) == factory.Converter(map, targetType));
		}
	}
}
