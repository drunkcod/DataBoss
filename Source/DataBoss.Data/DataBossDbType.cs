using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data;
using System.Globalization;
using System.Reflection;
using System.Text;
using DataBoss.Data.Scripting;
using DataBoss.Data.SqlServer;
using DataBoss.Linq;

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
			Real = 5,
			Float = 6,
			Decimal = 7,
			Bit = 8,
			DateTime = 9,
			Date = 10,
			Time = 11,
			DateTimeOffset = 12,
			Guid = 13,

			Char = 16,
			VarChar = 17,
			NChar = 18,
			NVarChar = 19,
			Binary = 20,
			VarBinary = 21,
			RowVersion = 22,
			
			TagMask = 31,

			IsVariableSize = Char,
			IsNullable = 1 << 7,
		}

		static readonly (string TypeName, byte Width)[] FixedTypes = new(string, byte)[]
		{
			(null, 0),
			("tinyint", 1),
			("smallint", 2),
			("int", 4),
			("bigint", 8),
			("real", 4),
			("float", 8),
			("decimal", 10),
			("bit", 0),
			("datetime", 8),
			("date", 3),
			("time", 8),
			("datetimeoffset", 10),
			("uniqueidentifier", 16)
		};

		static readonly (string TypeName, byte Width)[] VariableSizeTypes = new(string, byte)[] {
			("char", 0),
			("varchar", 0),
			("nchar", 0),
			("nvarchar", 0),
			("binary", 0),
			("varbinary", 0),
			//rowversion
			("binary", 0),
		};

		(string TypeName, byte Width) GetBossType(BossTypeTag tag) => tag.HasFlag(BossTypeTag.IsVariableSize)
			? VariableSizeTypes[(byte)(tag & BossTypeTag.TagMask) - (byte)BossTypeTag.Char]
			: FixedTypes[(byte)(tag & BossTypeTag.TagMask)];

		readonly BossTypeTag tag;
		readonly object extra;

		public int? ColumnSize => tag.HasFlag(BossTypeTag.IsVariableSize)
			? (int?)extra
			: IsKnownType(out var knownType) ? GetBossType(knownType).Width : null; 

		public string TypeName => IsKnownType(out var knownType) 
			? GetBossType(knownType).TypeName
			: CustomInfo.TypeName;

		readonly bool IsKnownType(out BossTypeTag typeTag) {
			typeTag = tag & BossTypeTag.TagMask;
			return typeTag != BossTypeTag.Custom;
		}

		(string TypeName, int? Width) CustomInfo => (ValueTuple<string, int?>)extra;

		public readonly bool IsRowVersion => (tag & BossTypeTag.TagMask) == BossTypeTag.RowVersion;

		public readonly bool IsNullable => tag.HasFlag(BossTypeTag.IsNullable);

		public static DataBossDbType Create(string typeName, int? columnSize, bool isNullable) {
			var tag = TypeTagLookup(ref typeName);
			if(tag == BossTypeTag.Custom)
				return new DataBossDbType(tag, isNullable, (typeName, columnSize));
			return new DataBossDbType(tag, isNullable, columnSize);
		}

		static BossTypeTag TypeTagLookup(ref string typeName) {
			var nameToFind = typeName;
			var n = Array.FindIndex(FixedTypes, x => x.TypeName == nameToFind);
			if(n == -1) { 
				n = Array.FindIndex(VariableSizeTypes, x => x.TypeName == nameToFind);
				if(n != -1)
					n += (int)BossTypeTag.IsVariableSize;
			}
			if (n == -1)
				return BossTypeTag.Custom;
			typeName = null;
			return (BossTypeTag)n;
		}

		DataBossDbType(BossTypeTag tag, bool isNullable) : this(tag, isNullable, null)
		{ }

		DataBossDbType(BossTypeTag tag, bool isNullable, object extra) {
			this.tag = tag | (isNullable ? BossTypeTag.IsNullable : 0);
			this.extra = extra;
		}

		public static DataBossDbType From(Type type) => From(type, type);

		public static DataBossDbType From(Type type, ICustomAttributeProvider attributes) {
			var canBeNull = !type.IsValueType && !attributes.Any<RequiredAttribute>();
			if (type.TryGetNullableTargetType(out var newTargetType)) {
				canBeNull = true;
				type = newTargetType;
			}
			return MapType(type, attributes, canBeNull);
		}

		public static DataBossDbType ToDataBossDbType(IDbDataParameter parameter) { 
			var t = MapType(parameter.DbType);
			return t.HasFlag(BossTypeTag.IsVariableSize)
			? new DataBossDbType(t, true, parameter.Size)
			: new DataBossDbType(t, true);
		}

		public string FormatValue(object value) {
			switch(tag & BossTypeTag.TagMask) { 
				default: throw new NotSupportedException($"Can't format {value} of type {value.GetType()} as {ToString()}");
				case BossTypeTag.TinyInt: return ChangeType<byte>(value).ToString();
				case BossTypeTag.SmallInt: return ChangeType<short>(value).ToString();
				case BossTypeTag.Int: return ChangeType<int>(value).ToString();
				case BossTypeTag.BigInt: return ChangeType<long>(value).ToString();
				case BossTypeTag.Real: return ChangeType<float>(value).ToString(CultureInfo.InvariantCulture);
				case BossTypeTag.Float: return ChangeType<double>(value).ToString(CultureInfo.InvariantCulture);
				case BossTypeTag.Decimal: return ChangeType<decimal>(value).ToString(CultureInfo.InvariantCulture);
				case BossTypeTag.DateTime: return ChangeType<DateTime>(value).ToString("s");
				case BossTypeTag.VarChar: return $"'{Escape(value.ToString())}'";
				case BossTypeTag.NVarChar: return $"N'{Escape(value.ToString())}'";
				case BossTypeTag.RowVersion:
					value = ((RowVersion)value).Value.Value;
					goto case BossTypeTag.VarBinary;
				case BossTypeTag.Binary:
				case BossTypeTag.VarBinary:
					var bytes = value as IEnumerable<byte>;
					if(bytes == null)
						goto default;
					var r = new StringBuilder("0x");
					foreach(var b in bytes)
						r.AppendFormat("{0:x2}", b);
					return r.ToString();
			}
		}

		static string Escape(string input) => input.Replace("'", "''");

		static T ChangeType<T>(object value) => (T)Convert.ChangeType(value, typeof(T));

		static BossTypeTag MapType(DbType dbType) {
			switch(dbType) {
				default: throw new NotSupportedException($"No mapping for {dbType}.");
				case DbType.Byte: return BossTypeTag.TinyInt;
				case DbType.Int16: return BossTypeTag.SmallInt;
				case DbType.Int32: return BossTypeTag.Int;
				case DbType.Int64: return BossTypeTag.BigInt;
				case DbType.Double: return BossTypeTag.Real;
				case DbType.Decimal: return BossTypeTag.Decimal;
				case DbType.Boolean: return BossTypeTag.Bit;
				case DbType.String: return BossTypeTag.NVarChar;
				case DbType.DateTime: return BossTypeTag.DateTime;
				case DbType.DateTimeOffset: return BossTypeTag.DateTimeOffset;
				case DbType.Binary: return BossTypeTag.Binary;
				case DbType.Guid: return BossTypeTag.Guid;
			}
		}

		internal static DataBossDbType MapType(Type type, ICustomAttributeProvider attributes, bool canBeNull) {

			var column = attributes.SingleOrDefault<ColumnAttribute>();
			if (column != null && !string.IsNullOrEmpty(column.TypeName))
				return Create(column.TypeName, null, canBeNull);

			var typeMapping = type.SingleOrDefault<TypeMappingAttribute>();
			if(typeMapping != null && !string.IsNullOrEmpty(typeMapping.TypeName))
				return Create(typeMapping.TypeName, null, canBeNull);

			switch (type.FullName) {
				case "System.Byte": return new DataBossDbType(BossTypeTag.TinyInt, canBeNull);
				case "System.Data.SqlTypes.SqlByte": return new DataBossDbType(BossTypeTag.TinyInt, canBeNull);
				case "System.Int16": return new DataBossDbType(BossTypeTag.SmallInt, canBeNull);
				case "System.Int32": return new DataBossDbType(BossTypeTag.Int, canBeNull);
				case "System.Int64": return new DataBossDbType(BossTypeTag.BigInt, canBeNull);
				case "System.Single": return new DataBossDbType(BossTypeTag.Real, canBeNull);
				case "System.Double": return new DataBossDbType(BossTypeTag.Float, canBeNull);
				case "System.Decimal": return new DataBossDbType(BossTypeTag.Decimal, canBeNull);
				case "System.Boolean": return new DataBossDbType(BossTypeTag.Bit, canBeNull);
				case "System.Guid": return new DataBossDbType(BossTypeTag.Guid, canBeNull);
				case "System.String":
					return new DataBossDbType(attributes.Any<AnsiStringAttribute>() ? BossTypeTag.VarChar: BossTypeTag.NVarChar, canBeNull, MaxLength(attributes)?.Length ?? int.MaxValue);
				case "System.Char":
					return new DataBossDbType(attributes.Any<AnsiStringAttribute>() ? BossTypeTag.Char : BossTypeTag.NChar, canBeNull, 1);
				case "System.Byte[]":
					return new DataBossDbType(BossTypeTag.VarBinary, canBeNull, MaxLength(attributes)?.Length ?? int.MaxValue);
				case "System.DateTime": return Create("datetime", 8, canBeNull);
				case "System.DateTimeOffset": return Create("datetimeoffset", 10, canBeNull);
				case "System.TimeSpan": return Create("time", 3, canBeNull);
				case "System.Data.SqlTypes.SqlMoney": return Create("money", null, canBeNull);
				case "DataBoss.Data.SqlServer.RowVersion": return new DataBossDbType(BossTypeTag.RowVersion, canBeNull, (int?)8);
				default:
					throw new NotSupportedException("Don't know how to map " + type.FullName + " to a db type.\nTry providing a TypeName using System.ComponentModel.DataAnnotations.Schema.ColumnAttribute.");
			}
		}

		static MaxLengthAttribute MaxLength(ICustomAttributeProvider attributes) =>
			attributes.SingleOrDefault<MaxLengthAttribute>();

		public static DbType ToDbType(Type type) => 
			type.FullName switch {
				"System.Byte" => DbType.Byte,
				"System.Int16" => DbType.Int16,
				"System.Int32" => DbType.Int32,
				"System.Int64" => DbType.Int64,
				"System.Single" => DbType.Single,
				"System.Double" => DbType.Double,
				"System.Decimal" => DbType.Decimal,
				"System.Boolean" => DbType.Boolean,
				"System.DateTime" => DbType.DateTime,
				"System.DateTimeOffset" => DbType.DateTimeOffset,
				"System.Guid" => DbType.Guid,
				"System.String" => DbType.String,
				_ => DbType.Object,
			};

		public static bool operator==(DataBossDbType a, DataBossDbType b) =>
			a.TypeName == b.TypeName && a.IsNullable == b.IsNullable;

		public static bool operator!=(DataBossDbType a, DataBossDbType b) => !(a == b);

		public override string ToString() => FormatType() + (IsNullable ? string.Empty : " not null");

		public override int GetHashCode() => TypeName.GetHashCode();

		public override bool Equals(object obj) => (obj is DataBossDbType && this == (DataBossDbType)obj) || obj.Equals(this);

		string FormatType() =>
			tag.HasFlag(BossTypeTag.IsVariableSize) ? FormatWideType() : TypeName;

		string FormatWideType() =>
			(!ColumnSize.HasValue || ColumnSize.Value == 1) ? TypeName : $"{TypeName}({FormatWidth(ColumnSize.Value)})";

		static string FormatWidth(int width) => width == int.MaxValue ? "max" : width.ToString();

	}
}