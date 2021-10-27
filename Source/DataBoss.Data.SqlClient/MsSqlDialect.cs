#if MSSQLCLIENT
namespace DataBoss.Data.MsSql
{
	using Microsoft.Data.SqlClient;
#else
namespace DataBoss.Data
{
	using System.Data.SqlClient;
#endif

	using System;
	using System.Data;
	using System.Linq.Expressions;
	using DataBoss.Data.SqlServer;
	using DataBoss.Linq.Expressions;
	using System.Collections.Concurrent;
	using System.Linq;
	using System.Data.SqlTypes;

	public class MsSqlDialect : ISqlDialect
	{
		static class Extractor<TArg>
		{
			internal static Action<SqlCommand, TArg> CreateParameters = 
				ToParams.CompileExtractor<SqlCommand, TArg>(Instance);
		}

		static readonly ConcurrentDictionary<Type, Action<SqlCommand, object>> Extractors = new();

		public static void AddParameters(SqlCommand command, object args) {
			if (args is null)
				return;
			Extractors.GetOrAdd(args.GetType(), CreateExtractor)(command, args);
		}

		static Action<SqlCommand, object> CreateExtractor(Type argsType) {
			var arg0 = Expression.Parameter(typeof(SqlCommand));
			var arg1 = Expression.Parameter(typeof(object));
			var addParameters = typeof(MsSqlDialect).GetMethods()
				.Single(x => x.Name == nameof(MsSqlDialect.AddParameters) && x.IsGenericMethodDefinition);
			var body = Expression.Call(
				addParameters.MakeGenericMethod(argsType),
				arg0,
				Expression.Convert(arg1, argsType));
			return Expression.Lambda<Action<SqlCommand, object>>(body, arg0, arg1).Compile();
		}

		private MsSqlDialect()  { }

		public static readonly MsSqlDialect Instance = new();
		
		public static void AddParameters<T>(SqlCommand cmd, T args) =>
			Extractor<T>.CreateParameters(cmd, args);

		public string ParameterPrefix => "@";

		public string GetTypeName(DataBossDbType dbType) => dbType.ToString();

		public bool TryCreateDialectSpecificParameter(string name, Expression readMember, out Expression create) {
			if(TryGetParameterPrototype(readMember.Type, out var createProto)) {
				create = Rebind(createProto, Expression.Constant(name), readMember);
				return true;
			} else if(readMember.Type.TryGetNullableTargetType(out var valueType) && TryGetParameterPrototype(valueType, out createProto)) {
				var value = Expression.Variable(readMember.Type, "value");
				var body = Expression.Block(
					new[] { value },
					Expression.Assign(value, readMember),
					Expression.Condition(
						Expression.Property(value, "HasValue"),
						Expression.Convert(Expression.Property(value, "Value"), typeof(object)),
						Expression.Constant(DBNull.Value, typeof(object))));
				create = Rebind(createProto, Expression.Constant(name), body);
				return true;
			}

			create = null;
			return false;
		}

		static bool TryGetParameterPrototype(Type type, out LambdaExpression found) {
			if(type == typeof(SqlDecimal))
				found = CreateSqlDecimalParameter;
			else if (type == typeof(SqlMoney))
				found = CreateSqlMoneyParameter;
			else if (type == typeof(SqlBinary))
				found = CreateSqlBinaryParameter;
			else if (type == typeof(RowVersion))
				found = CreateRowVersionParameter;
			else if (typeof(ITableValuedParameter).IsAssignableFrom(type))
				found = CreateTableValuedParameter;
			else found = null;
			return found != null;
		}

		static Expression Rebind(LambdaExpression expr, Expression arg0, Expression arg1) =>
			NodeReplacementVisitor.ReplaceParameters(expr, arg0, arg1);

		static readonly Expression<Func<string, object, SqlParameter>> CreateSqlDecimalParameter =
			(name, value) => new SqlParameter(name, SqlDbType.Decimal) { Value = value, };

		static readonly Expression<Func<string, object, SqlParameter>> CreateSqlMoneyParameter =
			(name, value) => new SqlParameter(name, SqlDbType.Money) { Value = value, };

		static readonly Expression<Func<string, object, SqlParameter>> CreateSqlBinaryParameter =
			(name, value) => new SqlParameter(name, SqlDbType.Binary) { Value = value, };

		static readonly Expression<Func<string, object, SqlParameter>> CreateRowVersionParameter = 
			(name, value) => new SqlParameter(name, SqlDbType.Binary, 8) { Value = value, };

		static readonly Expression<Func<string, ITableValuedParameter, SqlParameter>> CreateTableValuedParameter =
			(name, value) => new SqlParameter(name, SqlDbType.Structured) { 
				TypeName = value.TypeName, 
				Value = value.Rows
			};
	}
}