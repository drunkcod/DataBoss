using DataBoss.Data.Scripting;
using DataBoss.Data.SqlServer;
using DataBoss.Linq;
using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data;
using System.Linq.Expressions;
using System.Reflection;

namespace DataBoss.Data
{
	public struct DataBossDbType
	{
		enum BossTypeTag : byte
		{
			Custom = 0,
			TinyInt = 1,
			SmallInt = 2,
			Int = 3,
			BigInt = 4,
			Bit = 5,
			Char = 6,
			VarChar = 7,
			NChar = 8,
			NVarChar = 9,
			Binary = 10,
			VarBinary = 11,
			Real = 12,
			Float = 13,
			TagMask = 15,

			HasCoercion = 1 << 6,
			IsNullable = 1 << 7,
		}

		static readonly string[] BossTypes = new[]
		{
			null,
			"tinyint",
			"smallint",
			"int",
			"bigint",
			"bit",
			"char",
			"varchar",
			"nchar",
			"nvarchar",
			"binary",
			"varbinary",
			"real",
			"float"
		};

		static Func<Expression,Expression> Nop = x => x;
		static Func<Expression, Expression> CoerceRowVersion = x => Expression.PropertyOrField(x, nameof(RowVersion.Value));

		public string TypeName => BossTypes[(byte)(tag & BossTypeTag.TagMask)] ?? (string)extra;
		public readonly int? ColumnSize;
		public Func<Expression, Expression> Coerce => tag.HasFlag(BossTypeTag.HasCoercion) 
			? (Func<Expression,Expression>)extra 
			: Nop;
		public bool IsNullable => tag.HasFlag(BossTypeTag.IsNullable);
		readonly BossTypeTag tag;
		readonly object extra;

		public DataBossDbType(string typeName, int? columnSize, bool isNullable) : this(TypeTagLookup(ref typeName), columnSize, isNullable, typeName) 
		{ }

		static BossTypeTag TypeTagLookup(ref string typeName) {
			var n = Array.IndexOf(BossTypes, typeName);
			if(n == -1)
				return BossTypeTag.Custom;
			typeName = null;
			return (BossTypeTag)n;
		}

		DataBossDbType(BossTypeTag tag, int? columnSize, bool isNullable) : this(tag, columnSize, isNullable, null)
		{ }

		DataBossDbType(BossTypeTag tag, int? columnSize, bool isNullable, object extra) {
			this.ColumnSize = columnSize;
			this.tag = tag | (isNullable ? BossTypeTag.IsNullable : 0);
			this.extra = extra;
		}

		public static DataBossDbType ToDbType(Type type) => ToDbType(type, type);
		public static DataBossDbType ToDbType(Type type, ICustomAttributeProvider attributes) {
			var canBeNull = !type.IsValueType && !attributes.Any<RequiredAttribute>();
			if (type.TryGetNullableTargetType(out var newTargetType)) {
				canBeNull = true;
				type = newTargetType;
			}
			return MapType(type, attributes, canBeNull);
		}

		public static DataBossDbType ToDbType(IDbDataParameter parameter) =>
			new DataBossDbType(MapType(parameter.DbType), parameter.Size, true);

		static BossTypeTag MapType(DbType dbType) {
			switch(dbType) {
				default: throw new NotSupportedException($"No mapping for {dbType}.");
				case DbType.Byte: return BossTypeTag.TinyInt;
				case DbType.Int16: return BossTypeTag.SmallInt;
				case DbType.Int32: return BossTypeTag.Int;
				case DbType.Int64: return BossTypeTag.BigInt;
				case DbType.Boolean: return BossTypeTag.Bit;
				case DbType.String: return BossTypeTag.NVarChar;
				case DbType.Binary: return BossTypeTag.Binary;
			}
		}

		static DataBossDbType MapType(Type type, ICustomAttributeProvider attributes, bool canBeNull) {
			var column = attributes.SingleOrDefault<ColumnAttribute>();
			if (column != null && !string.IsNullOrEmpty(column.TypeName))
				return new DataBossDbType(column.TypeName, null, canBeNull);

			switch (type.FullName) {
				case "System.Byte": return new DataBossDbType(BossTypeTag.TinyInt, 1, canBeNull);
				case "System.Int16": return new DataBossDbType(BossTypeTag.SmallInt, 2, canBeNull);
				case "System.Int32": return new DataBossDbType(BossTypeTag.Int, 4, canBeNull);
				case "System.Int64": return new DataBossDbType(BossTypeTag.BigInt, 8, canBeNull);
				case "System.Single": return new DataBossDbType(BossTypeTag.Real, 4, canBeNull);
				case "System.Double": return new DataBossDbType(BossTypeTag.Float, 8, canBeNull);
				case "System.Boolean": return new DataBossDbType(BossTypeTag.Bit, 1, canBeNull);
				case "System.String":
					var maxLength = attributes.SingleOrDefault<MaxLengthAttribute>();
					return new DataBossDbType(attributes.Any<AnsiStringAttribute>() ? BossTypeTag.VarChar: BossTypeTag.NVarChar, maxLength?.Length ?? int.MaxValue, canBeNull);
				case "System.DateTime": return new DataBossDbType("datetime", 8, canBeNull);
				case "System.Data.SqlTypes.SqlMoney": return new DataBossDbType("money", null, canBeNull);
				case "DataBoss.Data.SqlServer.RowVersion": return new DataBossDbType(BossTypeTag.Binary | BossTypeTag.HasCoercion, 8, canBeNull, CoerceRowVersion);
				default:
					throw new NotSupportedException("Don't know how to map " + type.FullName + " to a db type.\nTry providing a TypeName using System.ComponentModel.DataAnnotations.Schema.ColumnAttribute.");
			}
		}

		public static SqlDbType ToSqlDbType(Type type) {
			switch(type.FullName) {
				case "System.Byte": return SqlDbType.TinyInt;
				case "System.Int16": return SqlDbType.SmallInt;
				case "System.Int32": return SqlDbType.Int;
				case "System.Int64": return SqlDbType.BigInt;
				case "System.Single": return SqlDbType.Real;;
				case "System.Double": return SqlDbType.Float;
				case "System.Boolean": return SqlDbType.Bit;
				case "System.DateTime": return SqlDbType.DateTime;
			}
			return SqlDbType.NVarChar;
		}

		public static bool operator==(DataBossDbType a, DataBossDbType b) =>
			a.TypeName == b.TypeName && a.IsNullable == b.IsNullable;

		public static bool operator!=(DataBossDbType a, DataBossDbType b) => !(a == b);

		public override string ToString() => FormatType() + (IsNullable ? string.Empty : " not null");

		public override int GetHashCode() => TypeName.GetHashCode();

		public override bool Equals(object obj) => (obj is DataBossDbType && this == (DataBossDbType)obj) || obj.Equals(this);

		string FormatType() => 
			IsWideType(tag) ? FormatWideType() : TypeName;

		string FormatWideType() =>
			(!ColumnSize.HasValue || ColumnSize.Value == 1) ? TypeName : $"{TypeName}({FormatWidth(ColumnSize.Value)})";

		static string FormatWidth(int width) => width == int.MaxValue ? "max" : width.ToString();

		static bool IsWideType(BossTypeTag tag) {
			switch(tag & BossTypeTag.TagMask) {
				default: return false;
				case BossTypeTag.Binary: return true;
				case BossTypeTag.VarBinary: return true;
				case BossTypeTag.Char: return true;
				case BossTypeTag.VarChar: return true;
				case BossTypeTag.NChar: return true;
				case BossTypeTag.NVarChar: return true;
			}
		}
	}
}