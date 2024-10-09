using System;
using System.Data;
using System.Linq.Expressions;
using System.Text.Json;
using System.Collections.Generic;
using DataBoss.Data.Support;
using Npgsql;
using NpgsqlTypes;
using System.Reflection;
using System.Text.Json.Serialization;
using System.Diagnostics.CodeAnalysis;

namespace DataBoss.Data.Npgsql
{
	public class NpgsqlDialect : SqlDialect<NpgsqlDialect, NpgsqlCommand>, ISqlDialect
	{
		static readonly JsonSerializerOptions JsonOptions = new() {
			DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
		};

		public static NpgsqlCustomParameterValue<byte[]> Json<T>(T value) => new(
			NpgsqlDbType.Json,
			JsonSerializer.SerializeToUtf8Bytes(value, JsonOptions));

		public static NpgsqlCustomParameterValue<byte[]> Jsonb<T>(T value) => new(
			NpgsqlDbType.Jsonb,
			JsonSerializer.SerializeToUtf8Bytes(value, JsonOptions));

		public string FormatName(string columnName) => $"\"{columnName}\"";
		
		public string GetTypeName(DataBossDbType dbType) => dbType.ToString();

		public bool TryCreateDialectSpecificParameter(string name, Expression readMember, out Expression? create) {
			if(readMember.Type == typeof(string)) {
				var (ctor, prop) = CreateParameter(name, typeof(string), DbType.String);
				create = Expression.MemberInit(
					ctor,
					Expression.Bind(prop, readMember));
			} else if(IsArrayLike(readMember.Type, out var itemType)) {
				if(TryMapDbType(itemType, out var dbType))
					create = NewNpgsqlParameter(readMember, name, NpgsqlDbType.Array | dbType);
				else 
					create = NewNpgsqlParameter(readMember, name, DbType.Object);
			}
			else if(readMember.Type.IsGenericType && readMember.Type.GetGenericTypeDefinition() == typeof(NpgsqlCustomParameterValue<>))
				create = Expression.Call(readMember, nameof(NpgsqlCustomParameterValue<object>.ToParameter), null, Expression.Constant(name));
			else
				create = default;

			return create != null;
		}

		static bool TryMapDbType(Type type, out NpgsqlDbType dbType) {
			if(type == typeof(string)) dbType = NpgsqlDbType.Text;
			else if(type == typeof(int)) dbType = NpgsqlDbType.Integer;
			else if(type == typeof(long)) dbType = NpgsqlDbType.Bigint;
			else if(type == typeof(short)) dbType = NpgsqlDbType.Smallint;
			else if(type == typeof(double)) dbType = NpgsqlDbType.Double;
			else if(type == typeof(float)) dbType = NpgsqlDbType.Real;
			else if(type == typeof(byte[])) dbType = NpgsqlDbType.Bytea;
			else if(type == typeof(Guid)) dbType = NpgsqlDbType.Uuid;
			else dbType = default;

			return dbType != default;
		}

		public (NewExpression?, PropertyInfo) CreateParameter(string name, Type type, DbType dbType) {
			var t = typeof(NpgsqlParameter<>).MakeGenericType(type);
			var ctor = t.GetConstructor(new[] { typeof(string), typeof(DbType) });
			
			var p = Expression.New(ctor, Expression.Constant(name), Expression.Constant(dbType));
			return (p, t.GetProperty("TypedValue"));
		}

		public bool SupportsNullable => true;
		public bool EnsureDBNull => false;

		public IReadOnlyList<string> DataBossHistoryMigrations => new[] { 
			@"create table __DataBossHistory(
				Id bigint not null,
				Context varchar(64) not null,
				Name text not null,
				StartedAt timestamp with time zone not null,
				FinishedAt timestamp with time zone,
				""User"" text,
				MigrationHash bytea,
				constraint PK_DataBossHistory primary key (Id, Context)
			)", 
		};

		public string BeginMigrationQuery =>
			@"insert into __DataBossHistory(Id, Context, Name, StartedAt, ""User"", MigrationHash)
			values(:id, :context, :name, now(), :user, :hash)
			on conflict on constraint PK_DataBossHistory
			do update set StartedAt = now(), FinishedAt = null, MigrationHash = :hash";

		public string EndMigrationQuery => "update __DataBossHistory set FinishedAt = now() where Id = :id and Context = :Context";


		private static bool IsArrayLike(Type type,[NotNullWhen(true)] out Type? itemType) { 
			if(type.IsArray) {
				itemType = type.GetElementType();
				return true;
			}
			var enumerable = type.GetInterface("System.Collections.Generic.IEnumerable`1") 
				?? (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(IEnumerable<>) ? type : null);
			
			if(enumerable is not null) {
				itemType = enumerable.GetGenericArguments()[0];
				return true;
			}
			itemType = null;
			return false;
		}

		static Expression NewNpgsqlParameter(Expression value, string name, DbType dbType) {
			var t = typeof(NpgsqlParameter);
			var ctor = t.GetConstructor(new[] { typeof(string), typeof(DbType) });
			return Expression.MemberInit(
				Expression.New(ctor, Expression.Constant(name), Expression.Constant(dbType)),
				Expression.Bind(t.GetProperty("Value"),
					Expression.Coalesce(Expression.Convert(value, typeof(object)), Expression.Constant(DBNull.Value))));
		}

		static Expression NewNpgsqlParameter(Expression value, string name, NpgsqlDbType dbType) {
			var t = typeof(NpgsqlParameter<>).MakeGenericType(value.Type);
			var ctor = t.GetConstructor(new[] { typeof(string), typeof(NpgsqlDbType) });
			return Expression.MemberInit(
				Expression.New(ctor, Expression.Constant(name), Expression.Constant(dbType)),
				Expression.Bind(t.GetProperty("TypedValue"), value));
		}

	}

	public readonly struct NpgsqlCustomParameterValue<T>
	{
		readonly NpgsqlDbType dbType;
		readonly T value;

		public NpgsqlCustomParameterValue(NpgsqlDbType dbType, T value) => (this.dbType, this.value) = (dbType, value);
		public NpgsqlParameter ToParameter(string name) => new NpgsqlParameter<T>(name, dbType) { TypedValue = value };
	}
}
