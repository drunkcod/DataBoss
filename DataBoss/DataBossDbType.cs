namespace DataBoss
{
	public struct DataBossDbType
	{
		public readonly string TypeName;
		public readonly int? ColumnSize;
		public readonly bool IsNullable;

		public DataBossDbType(string name, int? columnSize, bool isNullable) {
			this.TypeName = name;
			this.ColumnSize = columnSize;
			this.IsNullable = isNullable;
		}

		public static bool operator==(DataBossDbType a, DataBossDbType b) =>
			a.TypeName == b.TypeName && a.IsNullable == b.IsNullable;

		public static bool operator!=(DataBossDbType a, DataBossDbType b) => !(a == b);

		public override string ToString() => FormatType() + (IsNullable ? string.Empty : " not null");

		string FormatType() {
			if(IsWideType(TypeName)) return FormatWideType();
			return TypeName;
		}

		string FormatWideType() =>
			(!ColumnSize.HasValue || ColumnSize.Value == 1) ? TypeName : $"{TypeName}({FormatWidth(ColumnSize.Value)})";

		static string FormatWidth(int width) => width == int.MaxValue ? "max" : width.ToString();

		static bool IsWideType(string typeName) {
			switch(typeName) {
				default: return false;
				case "binary": return true;
				case "varbinary": return true;
				case "char": return true;
				case "varchar": return true;
				case "nchar": return true;
				case "nvarchar": return true;
			}
		}
	}
}