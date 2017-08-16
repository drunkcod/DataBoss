using Cone;
using Cone.Core;
using System;
using System.Linq;
using System.Data.SqlTypes;
using System.Data;

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

		public void nullable_values() => Check.With(() => 
			ToParams.Invoke(new {
				HasValue = new int?(1),
				NoInt32 = new int?(),
			}))
			.That(
				paras => paras.Length == 2,
				paras => paras[0].Value.Equals(1),
				paras => paras[0].SqlDbType == SqlDbType.Int,
				paras => paras[1].Value == DBNull.Value,
				paras => paras[1].SqlDbType == SqlDbType.Int);
	}
}
