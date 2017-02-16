using Cone;
using Cone.Core;
using System;
using System.Linq;
using System.Data.SqlTypes;

namespace DataBoss.Specs
{
	[Describe(typeof(ToParams))]
	public class ToParamsSpec
	{
		public void complext_type() =>
			Check.With(() => ToParams.Invoke(new { Args = new { Foo = 1, Bar = "Hello" } }))
				.That(
					x => x.Length == 2,
					x => x.Any(p => p.ParameterName == "@Args_Foo"),
					x => x.Any(p => p.ParameterName == "@Args_Bar"));

		[Row(typeof(string))
		,Row(typeof(Guid))
		,Row(typeof(DateTime))
		,Row(typeof(Decimal))
		,Row(typeof(SqlMoney))
		,Row(typeof(SqlDecimal))
		,Row(typeof(byte[]))
		,DisplayAs("{0}", Heading = "has sql type mapping for {0}")]
		public void has_sql_type_mapping_for(Type clrType) => Check.That(() => ToParams.HasSqlTypeMapping(clrType));

		public void object_is_not_considered_complext() {
			var nullableInt = new int?();
			Check.With(() => ToParams.Invoke(new { Value = nullableInt.HasValue ? (object)nullableInt.Value : DBNull.Value }))
				.That(x => x.Length == 1, x => x.Any(p => p.ParameterName == "@Value"));
		}
	}
}
