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

	public class MsSqlDialect : ISqlDialect
	{
		private MsSqlDialect()  { }

		public static readonly MsSqlDialect Instance = new();
		
		public string ParameterPrefix => "@";

		public string GetTypeName(DataBossDbType dbType) => dbType.ToString();

		public bool TryCreateParameter(string name, Expression readMember, out Expression create) {
			if(TryGetParameterPrototype(readMember.Type, out var createProto)) {
				create = Rebind(createProto, Expression.Constant(name), readMember);
				return true;
			}

			create = null;
			return false;
		}

		static bool TryGetParameterPrototype(Type type, out LambdaExpression found) {
			if (type == typeof(RowVersion))
				found = CreateRowVersionParameter;
			else if (typeof(ITableValuedParameter).IsAssignableFrom(type))
				found = CreateTableValuedParameter;
			else found = null;
			return found != null;
		}

		static Expression Rebind(LambdaExpression expr, Expression arg0, Expression arg1) =>
			NodeReplacementVisitor.ReplaceParameters(expr, arg0, arg1);

		static readonly Expression<Func<string, RowVersion, SqlParameter>> CreateRowVersionParameter = 
			(name, value) => new SqlParameter(name, SqlDbType.Binary, 8) { Value = value, };

		static readonly Expression<Func<string, ITableValuedParameter, SqlParameter>> CreateTableValuedParameter =
			(name, value) => new SqlParameter(name, SqlDbType.Structured) { 
				TypeName = value.TypeName, 
				Value = value.Rows
			};
	}
}