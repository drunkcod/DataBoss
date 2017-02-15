using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Reflection;
using DataBoss.Core;

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

		public static DataBossDbType ToDbType(Type type, ICustomAttributeProvider attributes) {
			var canBeNull = !type.IsValueType && !attributes.Any<RequiredAttribute>();
			if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(Nullable<>)) {
				canBeNull = true;
				type = type.GenericTypeArguments[0];
			}
			return MapType(type, attributes, canBeNull);
		}

		static DataBossDbType MapType(Type type, ICustomAttributeProvider attributes, bool canBeNull) {
			var column = attributes.SingleOrDefault<ColumnAttribute>();
			if (column != null && !string.IsNullOrEmpty(column.TypeName))
				return new DataBossDbType(column.TypeName, null, canBeNull);

			switch (type.FullName) {
				case "System.Int32": return new DataBossDbType("int", 4, canBeNull);
				case "System.Int64": return new DataBossDbType("bigint", 8, canBeNull);
				case "System.Single": return new DataBossDbType("real", 4, canBeNull);
				case "System.Double": return new DataBossDbType("float", 8, canBeNull);
				case "System.Boolean": return new DataBossDbType("bit", 1, canBeNull);
				case "System.String":
					var maxLength = attributes.SingleOrDefault<MaxLengthAttribute>();
					return new DataBossDbType("varchar", maxLength?.Length ?? int.MaxValue, canBeNull);
				case "System.DateTime": return new DataBossDbType("datetime", 8, canBeNull);
				case "System.Data.SqlTypes.SqlMoney": return new DataBossDbType("money", null, canBeNull);
				default:
					throw new NotSupportedException("Don't know how to map " + type.FullName + " to a db type.\nTry providing a TypeName using System.ComponentModel.DataAnnotations.Schema.ColumnAttribute.");
			}
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