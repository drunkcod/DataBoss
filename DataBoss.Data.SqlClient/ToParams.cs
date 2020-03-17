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
	using System.Collections;
	using System.Collections.Generic;
	using System.Data;
	using System.Data.SqlTypes;
	using System.Linq;
	using System.Linq.Expressions;
	using System.Reflection;
	using DataBoss.Data.SqlServer;
	using DataBoss.Linq;

	public static class ToParams
	{
		static HashSet<Type> mappedTypes = new HashSet<Type> {
			typeof(object),
			typeof(string),
			typeof(DateTime),
			typeof(decimal),
			typeof(Guid),
			typeof(SqlDecimal),
			typeof(SqlMoney),
			typeof(byte[]),
			typeof(SqlBinary),
		};

		static class Extractor<TCommand, TArg>
		{
			internal static Action<TCommand, TArg> CreateParameters = 
				(Action<TCommand, TArg>)CreateExtractor(typeof(TCommand), typeof(TArg))
				.Compile();
		}

		static LambdaExpression CreateExtractor(Type commandType, Type argType) {
			var command = Expression.Parameter(commandType);
			var args = Expression.Parameter(argType);

			var extractor = ExtractorContext.For(command);
			ExtractValues(extractor, "@", args);
			return Expression.Lambda(extractor.GetResult(), command, args);
		}

		class ExtractorContext
		{
			readonly PropertyInfo parameterName = typeof(IDataParameter).GetProperty(nameof(IDataParameter.ParameterName));
			readonly PropertyInfo parameterValue = typeof(IDataParameter).GetProperty(nameof(IDbDataParameter.Value));

			readonly MethodInfo createParameter;
			readonly PropertyInfo getParameters;
			readonly MethodInfo addParameter;
			readonly List<Expression> extractedValues = new List<Expression>();

			public readonly Expression Target;

			public Type TypeOfParameters => createParameter.ReturnType;

			ExtractorContext(Expression target, MethodInfo createParameter, PropertyInfo getParameters, MethodInfo addParameter) {
				this.Target = target;
				this.createParameter = createParameter;
				this.getParameters = getParameters;
				this.addParameter = addParameter;
			}

			public Expression GetResult() => Expression.Block(extractedValues.Concat(new[] { Expression.Empty() }));

			public static ExtractorContext For(Expression target) {
				var createParameter = target.Type.GetMethod(nameof(IDbCommand.CreateParameter), Type.EmptyTypes);
				var getParameters = target.Type.GetProperty(nameof(IDbCommand.Parameters), BindingFlags.DeclaredOnly | BindingFlags.Instance | BindingFlags.Public)
					?? typeof(IDbCommand).GetProperty(nameof(IDbCommand.Parameters));
				var addParameter = getParameters.PropertyType.GetMethod("Add", new[] { createParameter.ReturnType })
					?? typeof(IList).GetMethod(nameof(IList.Add));
				return new ExtractorContext(target, createParameter, getParameters, addParameter);
			}

			public ParameterExpression CreatParameter(string name) => 
				Expression.Variable(TypeOfParameters, name);

			public void AddParameter(ParameterExpression p, Expression initP) {
				var setP = Expression.Assign(p, CreateParameter());
				var nameP = Expression.Assign(
					Expression.MakeMemberAccess(p, parameterName),
					Expression.Constant(p.Name));

				initP = Expression.Assign(
					Expression.MakeMemberAccess(p, parameterValue),
					initP);

				extractedValues.Add(Expression.Block(new[] { p }, setP, nameP, initP, AddParameter(p)));
			}

			MethodCallExpression CreateParameter() => Expression.Call(Target, createParameter);
			Expression GetParameterCollection() => Expression.MakeMemberAccess(Target, getParameters);
			Expression AddParameter(Expression p) => Expression.Call(GetParameterCollection(), addParameter, p);
		}

		static void ExtractValues(ExtractorContext extractor, string prefix, Expression input) {
			foreach (var value in input.Type.GetProperties()
				.Where(x => x.CanRead)
				.Concat<MemberInfo>(input.Type.GetFields())
			) {
				var name = prefix + value.Name;
				var readMember = Expression.MakeMemberAccess(input, value);

				var p = extractor.CreatParameter(name);
				Expression initP = null;
				if (HasSqlTypeMapping(readMember.Type))
					initP = MakeParameter(readMember);
				else if (readMember.Type.IsNullable())
					initP = MakeParameterFromNullable(p, readMember);
				else if(readMember.Type == typeof(RowVersion))
					initP = MakeRowVersionParameter(p, readMember);
				else if(TryGetDbType(readMember, out var readAsDbType))
					initP = MakeParameter(readAsDbType);
				if(initP != null)
					extractor.AddParameter(p, initP);
				else
					ExtractValues(extractor, name + "_", readMember);
			}
		}

		static bool TryGetDbType(Expression readMember, out Expression readAsDbType) {
			var dbType = readMember.Type.SingleOrDefault<DbTypeAttribute>();
			if(dbType != null) {
				readAsDbType = Expression.Convert(readMember, dbType.Type);
				return true;
			}
			readAsDbType = null;
			return false;
		}

		public static bool HasSqlTypeMapping(Type t) => t.IsPrimitive || mappedTypes.Contains(t) || t.IsEnum;

		static Expression MakeParameter(Expression value) =>
			value.Type.IsClass 
			? (Expression)Expression.Coalesce(
				Expression.Convert(value, typeof(object)), 
				Expression.Constant(DBNull.Value, typeof(object)))
			: Expression.Convert(value, typeof(object));

		static Expression MakeParameterFromNullable(Expression p, Expression value) =>
			Expression.Condition(
				Expression.MakeMemberAccess(value, value.Type.GetProperty(nameof(Nullable<int>.HasValue))),
					Expression.Convert(Expression.MakeMemberAccess(value, value.Type.GetProperty(nameof(Nullable<int>.Value))), typeof(object)),
					Expression.Block(
						Expression.Assign(
							Expression.MakeMemberAccess(p, typeof(IDataParameter).GetProperty(nameof(IDataParameter.DbType))), 
								Expression.Constant(DataBossDbType.ToDbType(value.Type.GetGenericArguments()[0]))),
						Expression.Constant(DBNull.Value, typeof(object))));

		static Expression MakeRowVersionParameter(Expression p, Expression value) {
			var setType = p.Type == typeof(SqlParameter)
				? Expression.Assign(
					Expression.MakeMemberAccess(p, typeof(SqlParameter).GetProperty(nameof(SqlParameter.SqlDbType))),
					Expression.Constant(SqlDbType.Binary))
				: Expression.Assign(
					Expression.MakeMemberAccess(p, typeof(IDataParameter).GetProperty(nameof(IDataParameter.DbType))),
					Expression.Constant(DbType.Binary));

			var setValue = Expression.Convert(Expression.Field(value, nameof(RowVersion.Value)), typeof(object));

			return Expression.Block(setType, setValue);
		}

		public static void AddTo<TCommand, T>(TCommand command, T args) where TCommand : IDbCommand => 
			Extractor<TCommand,T>.CreateParameters(command, args);
	}
}