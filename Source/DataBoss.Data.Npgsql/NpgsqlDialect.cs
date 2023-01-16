using System;
using System.Data;
using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using DataBoss.Data.Support;
using DataBoss.Linq.Expressions;
using Npgsql;

namespace DataBoss.Data.Npgsql
{
	public class NpgsqlDialect : SqlDialect<NpgsqlDialect, NpgsqlCommand>, ISqlDialect
	{
		public string FormatName(string columnName) => columnName;
		
		public string GetTypeName(DataBossDbType dbType) => dbType.ToString();

		public bool TryCreateDialectSpecificParameter(string name, Expression readMember, out Expression? create) {
			if (TryGetParameterPrototype(readMember.Type, out var createProto)) {
			create = Rebind(createProto, Expression.Constant(name), readMember);
				return true;
			}

				create = default;
			return false;
		}

		static bool TryGetParameterPrototype(Type type, [NotNullWhen(returnValue: true)] out LambdaExpression? found) {
			if (type == typeof(string))
				found = CreateStringParameter;
			else found = null;
			return found != null;
		}

		static Expression Rebind(LambdaExpression expr, Expression arg0, Expression arg1) =>
			NodeReplacementVisitor.ReplaceParameters(expr, arg0, arg1);

		static readonly Expression<Func<string, string, NpgsqlParameter>> CreateStringParameter =
			(name, value) => new NpgsqlParameter(name, DbType.String) { Value = (object)value ?? DBNull.Value };
	}
}
