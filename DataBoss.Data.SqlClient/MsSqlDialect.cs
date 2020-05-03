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

	public class MsSqlDialect : ISqlDialect
	{
		private MsSqlDialect()  { }

		public static readonly MsSqlDialect Instance = new MsSqlDialect();
		
		public string ParameterPrefix => "@";

		public Expression MakeRowVersionParameter(string name, Expression readMember)=> 
			Expression.MemberInit(
				Expression.New(
					typeof(SqlParameter).GetConstructor(new []{ typeof(string), typeof(SqlDbType), typeof(int) }),
					Expression.Constant(name),
					Expression.Constant(SqlDbType.Binary),
					Expression.Constant(8)), 
					Expression.Bind(
						typeof(SqlParameter).GetProperty(nameof(SqlParameter.Value)),
						Expression.Convert(Expression.Field(readMember, nameof(RowVersion.Value)), typeof(object))));



		public static void AddTo<T>(SqlCommand command, T args) =>
			Extractor<T>.CreateParameters(command, args);

		static class Extractor<TArg>
		{
			internal static Action<SqlCommand, TArg> CreateParameters =
				(Action<SqlCommand, TArg>)ToParams.CreateExtractor(Instance, typeof(SqlCommand), typeof(TArg), typeof(TArg))
				.Compile();
		}
	}
}