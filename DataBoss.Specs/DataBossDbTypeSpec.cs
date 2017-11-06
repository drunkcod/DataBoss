using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data.SqlTypes;
using Cone;
using DataBoss.Data;

namespace DataBoss.Specs
{
	[Describe(typeof(DataBossDbType))]
	public class DataBossDbTypeSpec
	{
		[Row("varchar", 100, "varchar(100)")
		,Row("varchar", int.MaxValue, "varchar(max)")
		,Row("char", 1, "char")
		,Row("char", 10, "char(10)")
		,Row("nvarchar", 100, "nvarchar(100)")
		,Row("nchar", 1, "nchar")
		,Row("nchar", 11, "nchar(11)")
		,Row("varbinary", 123, "varbinary(123)")
		,Row("binary", 1, "binary")
		,Row("binary", 12, "binary(12)")]
		public void columnsize(string dataType, int columnSize, string expected) =>
			Check.That(() => new DataBossDbType(dataType, columnSize, true).ToString() == expected);

		[DisplayAs("{0} maps to db type {1}")
		,Row(typeof(DateTime?), "datetime", true)
		,Row(typeof(DateTime), "datetime", false)
		,Row(typeof(byte?), "tinyint", true)
		,Row(typeof(byte), "tinyint", false)
		,Row(typeof(short?), "smallint", true)
		,Row(typeof(short), "smallint", false)
		,Row(typeof(int?), "int", true)
		,Row(typeof(int), "int", false)
		,Row(typeof(long?), "bigint", true)
		,Row(typeof(long), "bigint", false)
		,Row(typeof(float?), "real", true)
		,Row(typeof(float), "real", false)
		,Row(typeof(double?), "float", true)
		,Row(typeof(double), "float", false)
		,Row(typeof(string), "varchar(max)", true)
		,Row(typeof(bool), "bit", false)
		,Row(typeof(bool?), "bit", true)
		,Row(typeof(SqlMoney), "money", false)
		,Row(typeof(SqlMoney?), "money", true)]
		public void to_db_type(Type type, string dbType, bool nullable) =>
			Check.That(() => DataBossDbType.ToDbType(type, new StubAttributeProvider()).ToString() == new DataBossDbType(dbType, null, nullable).ToString());

		#pragma warning disable CS0649
		class MyRowType
		{
			[Column(TypeName = "decimal(18, 5)")]
			public decimal Value;
		}
		#pragma warning restore CS0649

		public void to_db_type_with_column_type_override() {
			var column = typeof(MyRowType).GetField(nameof(MyRowType.Value));
			Check.That(() => DataBossDbType.ToDbType(column.FieldType, column) == new DataBossDbType("decimal(18, 5)", null, false));
		}

		public void RequiredAttribute_string_is_not_null() {
			Check.That(() => DataBossDbType.ToDbType(typeof(string), new StubAttributeProvider().Add(new RequiredAttribute())) == new DataBossDbType("varchar", int.MaxValue, false));
		}

		public void MaxLengthAttribute_controls_string_column_widht() {
			Check.That(() => DataBossDbType.ToDbType(typeof(string), new StubAttributeProvider().Add(new MaxLengthAttribute(31))) == new DataBossDbType("varchar", 31, true));
		}
	}
}
