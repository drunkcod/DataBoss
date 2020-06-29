using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using System.Globalization;
using System.Linq;
using Cone;
using DataBoss.Data.SqlServer;
using DataBoss.Specs;
using Xunit;

namespace DataBoss.Data.Specs
{
	public class DataBossDbTypeSpec
	{
		[Theory]
		[InlineData("varchar", 100, "varchar(100)")]
		[InlineData("varchar", int.MaxValue, "varchar(max)")]
		[InlineData("char", 1, "char")]
		[InlineData("char", 10, "char(10)")]
		[InlineData("nvarchar", 100, "nvarchar(100)")]
		[InlineData("nchar", 1, "nchar")]
		[InlineData("nchar", 11, "nchar(11)")]
		[InlineData("varbinary", 123, "varbinary(123)")]
		[InlineData("binary", 1, "binary")]
		[InlineData("binary", 12, "binary(12)")]
		public void columnsize(string dataType, int columnSize, string expected) =>
			Check.That(() => DataBossDbType.Create(dataType, columnSize, true).ToString() == expected);

		[Theory]
		[InlineData(typeof(DateTime?), "datetime", true)]
		[InlineData(typeof(DateTime), "datetime", false)]
		[InlineData(typeof(byte?), "tinyint", true)]
		[InlineData(typeof(byte), "tinyint", false)]
		[InlineData(typeof(short?), "smallint", true)]
		[InlineData(typeof(short), "smallint", false)]
		[InlineData(typeof(int?), "int", true)]
		[InlineData(typeof(int), "int", false)]
		[InlineData(typeof(long?), "bigint", true)]
		[InlineData(typeof(long), "bigint", false)]
		[InlineData(typeof(float?), "real", true)]
		[InlineData(typeof(float), "real", false)]
		[InlineData(typeof(double?), "float", true)]
		[InlineData(typeof(double), "float", false)]
		[InlineData(typeof(string), "nvarchar(max)", true)]
		[InlineData(typeof(bool), "bit", false)]
		[InlineData(typeof(bool?), "bit", true)]
		[InlineData(typeof(SqlMoney), "money", false)]
		[InlineData(typeof(SqlMoney?), "money", true)]
		[InlineData(typeof(RowVersion), "binary(8)", false)]
		public void to_db_type(Type type, string dbType, bool nullable) =>
			Check.That(() => DataBossDbType.ToDataBossDbType(type, new StubAttributeProvider()).ToString() == DataBossDbType.Create(dbType, null, nullable).ToString());

		#pragma warning disable CS0649
		class MyRowType
		{
			[Column(TypeName = "decimal(18, 5)")]
			public decimal Value;
		}
		#pragma warning restore CS0649

		[Fact]
		public void to_db_type_with_column_type_override() {
			var column = typeof(MyRowType).GetField(nameof(MyRowType.Value));
			Check.That(() => DataBossDbType.ToDataBossDbType(column.FieldType, column) == DataBossDbType.Create("decimal(18, 5)", null, false));
		}

		[Fact]
		public void RequiredAttribute_string_is_not_null() =>
			Check.That(() => DataBossDbType.ToDataBossDbType(typeof(string), new StubAttributeProvider().Add(new RequiredAttribute())) == DataBossDbType.Create("nvarchar", int.MaxValue, false));

		[Fact]
		public void MaxLengthAttribute_controls_string_column_widht()=>
			Check.That(() => DataBossDbType.ToDataBossDbType(typeof(string), new StubAttributeProvider().Add(new MaxLengthAttribute(31))) == DataBossDbType.Create("nvarchar", 31, true));

		void from_DbParameter(DbParameter parameter, string expected) =>
			Check.That(() => DataBossDbType.ToDataBossDbType(parameter).ToString() == expected);

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

		[Theory, MemberData(nameof(FormatValueRows))]
		public void format_value(DataBossDbType dbType, object value, string expected) =>
			Check.That(() => dbType.FormatValue(value) == expected);

		public static IEnumerable<object[]> FormatValueRows() =>
			new (DataBossDbType, object, string)[] {
				(DataBossDbType.ToDataBossDbType(typeof(byte)), byte.MinValue, byte.MinValue.ToString()),
				(DataBossDbType.ToDataBossDbType(typeof(byte)), byte.MaxValue, byte.MaxValue.ToString()),
				(DataBossDbType.ToDataBossDbType(typeof(short)), short.MinValue, short.MinValue.ToString()),
				(DataBossDbType.ToDataBossDbType(typeof(short)), short.MaxValue, short.MaxValue.ToString()),
				(DataBossDbType.ToDataBossDbType(typeof(int)), int.MinValue, int.MinValue.ToString()),
				(DataBossDbType.ToDataBossDbType(typeof(int)), int.MaxValue, int.MaxValue.ToString()),
				(DataBossDbType.ToDataBossDbType(typeof(long)), long.MinValue, long.MinValue.ToString()),
				(DataBossDbType.ToDataBossDbType(typeof(long)), long.MaxValue, long.MaxValue.ToString()),
				(DataBossDbType.ToDataBossDbType(typeof(float)), Math.PI, ((float)Math.PI).ToString(CultureInfo.InvariantCulture)),
				(DataBossDbType.ToDataBossDbType(typeof(double)), Math.E, Math.E.ToString(CultureInfo.InvariantCulture)),
				(DataBossDbType.ToDataBossDbType(typeof(DateTime)), new DateTime(2018, 07, 01, 13, 17, 31), "2018-07-01T13:17:31"),
				(DataBossDbType.ToDataBossDbType(typeof(string)), "Hello World", "N'Hello World'"),
				(DataBossDbType.ToDataBossDbType(typeof(string)), "'", "N''''"),
				(DataBossDbType.ToDataBossDbType(typeof(byte[])), new byte[]{ 1, 2, 3 }, "0x010203"),
				(DataBossDbType.ToDataBossDbType(typeof(RowVersion)), new RowVersion(new SqlBinary(new byte[]{ 1, 2, 3, 4, 5, 6, 7, 8 }).Value), "0x0102030405060708"),
			}.Select(x => new object[] { x.Item1, x.Item2, x.Item3 });

		[Theory, MemberData(nameof(FormatValueFailRows))]
		public void format_value_fail(DataBossDbType dbType, object value) =>
			Check.Exception<OverflowException>(() => dbType.FormatValue(value));

		public static IEnumerable<object[]> FormatValueFailRows() =>
			new[] {
				(DataBossDbType.ToDataBossDbType(typeof(byte)), 1024),
			}.Select(x => new object[] { x.Item1, x.Item2 });

		static SqlParameter Parameter(SqlDbType dbType, bool isNullable) =>
			new SqlParameter {
				IsNullable = isNullable,
				SqlDbType = dbType
			};

		static SqlParameter Parameter(object value) => new SqlParameter { Value = value };
	}
}
