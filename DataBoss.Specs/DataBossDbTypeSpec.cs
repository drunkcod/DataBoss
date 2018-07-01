using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using System.Linq;
using Cone;
using DataBoss.Data;
using DataBoss.Data.SqlServer;

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
			Check.That(() => DataBossDbType.Create(dataType, columnSize, true).ToString() == expected);

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
		,Row(typeof(string), "nvarchar(max)", true)
		,Row(typeof(bool), "bit", false)
		,Row(typeof(bool?), "bit", true)
		,Row(typeof(SqlMoney), "money", false)
		,Row(typeof(SqlMoney?), "money", true)
		,Row(typeof(RowVersion), "binary(8)", false)]
		public void to_db_type(Type type, string dbType, bool nullable) =>
			Check.That(() => DataBossDbType.ToDbType(type, new StubAttributeProvider()).ToString() == DataBossDbType.Create(dbType, null, nullable).ToString());

		#pragma warning disable CS0649
		class MyRowType
		{
			[Column(TypeName = "decimal(18, 5)")]
			public decimal Value;
		}
		#pragma warning restore CS0649

		public void to_db_type_with_column_type_override() {
			var column = typeof(MyRowType).GetField(nameof(MyRowType.Value));
			Check.That(() => DataBossDbType.ToDbType(column.FieldType, column) == DataBossDbType.Create("decimal(18, 5)", null, false));
		}

		public void RequiredAttribute_string_is_not_null() =>
			Check.That(() => DataBossDbType.ToDbType(typeof(string), new StubAttributeProvider().Add(new RequiredAttribute())) == DataBossDbType.Create("nvarchar", int.MaxValue, false));

		public void MaxLengthAttribute_controls_string_column_widht()=>
			Check.That(() => DataBossDbType.ToDbType(typeof(string), new StubAttributeProvider().Add(new MaxLengthAttribute(31))) == DataBossDbType.Create("nvarchar", 31, true));

		public void from_DbParameter(DbParameter parameter, string expected) =>
			Check.That(() => DataBossDbType.ToDbType(parameter).ToString() == expected);

		public IEnumerable<IRowTestData> DbParameterRows() => 
			new[] {
				(Parameter(SqlDbType.Int, isNullable: false), "int"),
				(Parameter(SqlDbType.Int, isNullable: true), "int"),
				(Parameter(SqlDbType.SmallInt, isNullable: true), "smallint"),
				(Parameter(SqlDbType.BigInt, isNullable: true), "bigint"),
				(Parameter(SqlDbType.TinyInt, isNullable: true), "tinyint"),
				(Parameter(false), "bit"),
				(Parameter("Hello"), "nvarchar(5)"),
				(Parameter(new byte[]{ 1, 2, 3, 4 }), "binary(4)")
			}.Select(x =>
				new RowTestData(new Cone.Core.Invokable(GetType().GetMethod(nameof(from_DbParameter))), 
				new object[]{ x.Item1, x.Item2 }));

		public void format_value(DataBossDbType dbType, object value, string expected) =>
			Check.That(() => dbType.FormatValue(value) == expected);

		public IEnumerable<IRowTestData> FormatValueRows() =>
			new[] {
				(DataBossDbType.ToDbType(typeof(byte)), byte.MinValue, byte.MinValue.ToString()),
				(DataBossDbType.ToDbType(typeof(byte)), byte.MaxValue, byte.MaxValue.ToString()),
				(DataBossDbType.ToDbType(typeof(short)), short.MinValue, short.MinValue.ToString()),
				(DataBossDbType.ToDbType(typeof(short)), short.MaxValue, short.MaxValue.ToString()),
				(DataBossDbType.ToDbType(typeof(int)), int.MinValue, int.MinValue.ToString()),
				(DataBossDbType.ToDbType(typeof(int)), int.MaxValue, int.MaxValue.ToString()),

			}.Select(x =>
				new RowTestData(new Cone.Core.Invokable(GetType().GetMethod(nameof(format_value))),
				new object[] { x.Item1, x.Item2, x.Item3 }));

		public void format_value_fail(DataBossDbType dbType, object value) =>
			Check.Exception<OverflowException>(() => dbType.FormatValue(value));

		public IEnumerable<IRowTestData> FormatValueFailRows() =>
			new[] {
				(DataBossDbType.ToDbType(typeof(byte)), 1024),
			}.Select(x =>
				new RowTestData(new Cone.Core.Invokable(GetType().GetMethod(nameof(format_value_fail))),
				new object[] { x.Item1, x.Item2 }));

		static SqlParameter Parameter(SqlDbType dbType, bool isNullable) =>
			new SqlParameter {
				IsNullable = isNullable,
				SqlDbType = dbType
			};

		static SqlParameter Parameter(object value) => new SqlParameter { Value = value };
	}
}
