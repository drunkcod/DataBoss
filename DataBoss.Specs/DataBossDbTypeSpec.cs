using Cone;

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
	}
}
