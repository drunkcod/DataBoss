using System;
using System.Data;
using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using System.Text.Json;
using DataBoss.Data.Support;
using DataBoss.Linq.Expressions;
using Npgsql;
using NpgsqlTypes;

namespace DataBoss.Data.Npgsql
{
	public class NpgsqlDialect : SqlDialect<NpgsqlDialect, NpgsqlCommand>, ISqlDialect
	{
		public static NpgsqlCustomParameterValue<byte[]> Json<T>(T value) => new(
			JsonSerializer.SerializeToUtf8Bytes(value),
			NpgsqlDbType.Json);

		public static NpgsqlCustomParameterValue<byte[]> Jsonb<T>(T value) => new(
			JsonSerializer.SerializeToUtf8Bytes(value), 
			NpgsqlDbType.Jsonb);

		public string FormatName(string columnName) => columnName;
		
		public string GetTypeName(DataBossDbType dbType) => dbType.ToString();

		public bool TryCreateDialectSpecificParameter(string name, Expression readMember, out Expression? create) {
			if (TryGetParameterPrototype(readMember.Type, out var createProto)) {
				create = Rebind(createProto, Expression.Constant(name), readMember);
				return true;
			}

			if(readMember.Type.IsGenericType && readMember.Type.GetGenericTypeDefinition() == typeof(NpgsqlCustomParameterValue<>)) {
				create = Expression.Call(readMember, nameof(NpgsqlCustomParameterValue<object>.ToParameter), null, Expression.Constant(name)); 
				return true;
			}

			create = default;
			return false;
		}

		static bool TryGetParameterPrototype(Type type, [NotNullWhen(returnValue: true)] out LambdaExpression? found) {
			if (type == typeof(string))
				found = CreateStringParameter;
			else if(type.IsArray)
				found = CreateArrayParameter;
			else found = null;

			return found != null;
		}

		static Expression Rebind(LambdaExpression expr, Expression arg0, Expression arg1) =>
			NodeReplacementVisitor.ReplaceParameters(expr, arg0, arg1);

		static readonly Expression<Func<string, string, NpgsqlParameter>> CreateStringParameter =
			(name, value) => new NpgsqlParameter(name, DbType.String) { Value = (object)value ?? DBNull.Value };

		static readonly Expression<Func<string, string, NpgsqlParameter>> CreateArrayParameter =
			(name, value) => new NpgsqlParameter(name, DbType.Object) { Value = (object)value ?? DBNull.Value };
	}

	public readonly struct NpgsqlCustomParameterValue<T>
	{
		readonly NpgsqlDbType dbType;
		readonly T value;

		public NpgsqlCustomParameterValue(T value, NpgsqlDbType dbType) { 
			this.dbType = dbType;
			this.value = value;
		}

		public NpgsqlParameter ToParameter(string name) => new NpgsqlParameter<T>(name, dbType) { TypedValue = value };
	}
}
