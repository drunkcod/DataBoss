using System;
using System.Data;
using System.Linq.Expressions;
using System.Text.Json;
using System.Collections.Generic;
using DataBoss.Data.Support;
using Npgsql;
using NpgsqlTypes;

namespace DataBoss.Data.Npgsql
{
	public class NpgsqlDialect : SqlDialect<NpgsqlDialect, NpgsqlCommand>, ISqlDialect
	{
		public static NpgsqlCustomParameterValue<byte[]> Json<T>(T value) => new(
			NpgsqlDbType.Json,
			JsonSerializer.SerializeToUtf8Bytes(value));

		public static NpgsqlCustomParameterValue<byte[]> Jsonb<T>(T value) => new(
			NpgsqlDbType.Jsonb,
			JsonSerializer.SerializeToUtf8Bytes(value));

		public string FormatName(string columnName) => columnName;
		
		public string GetTypeName(DataBossDbType dbType) => dbType.ToString();

		public bool TryCreateDialectSpecificParameter(string name, Expression readMember, out Expression? create) {
			if (readMember.Type == typeof(string))
				create = NewNpgsqlParameter(readMember, name, DbType.String);
			else if (IsArrayLike(readMember.Type))
				create = NewNpgsqlParameter(readMember, name, DbType.Object);
			else if (readMember.Type.IsGenericType && readMember.Type.GetGenericTypeDefinition() == typeof(NpgsqlCustomParameterValue<>))
				create = Expression.Call(readMember, nameof(NpgsqlCustomParameterValue<object>.ToParameter), null, Expression.Constant(name));
			else
				create = default;

			return create != null;
		}

		public IReadOnlyList<string> DataBossHistoryMigrations => new[] { 
			@"			create table __DataBossHistory(
				Id bigint not null,
				Context varchar(64) not null,
				Name text not null,
				StartedAt timestamp with time zone not null,
				FinishedAt timestamp with time zone not null,
				""User"" text,
				MigrationHash bytea,
				constraint PK_DataBossHistory primary key (Id, Context)
			)", 
		};


		private static bool IsArrayLike(Type type) => 
			type.IsArray || type.GetInterface("System.Collections.Generic.IEnumerable`1") is not null;

		static Expression NewNpgsqlParameter(Expression value, string name, DbType dbType) {
			var t = typeof(NpgsqlParameter);
			var ctor = t.GetConstructor(new[] { typeof(string), typeof(DbType) });
			return Expression.MemberInit(
				Expression.New(ctor, Expression.Constant(name), Expression.Constant(dbType)),
				Expression.Bind(t.GetProperty("Value"),
					Expression.Coalesce(Expression.Convert(value, typeof(object)), Expression.Constant(DBNull.Value))));
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
