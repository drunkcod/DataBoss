namespace DataBoss.Data
{
	using System;
	using System.Collections;
	using System.Collections.Generic;
	using System.Data;
	using System.Linq;
	using System.Linq.Expressions;
	using System.Reflection;
	using DataBoss.Expressions;
	using DataBoss.Linq;

	public static class ToParams
	{
		static readonly HashSet<Type> MappedTypes = new() {
			typeof(object),
			typeof(string),
			typeof(DateTime),
			typeof(decimal),
			typeof(Guid),
			typeof(byte[]),
		};

		public static LambdaExpression CreateExtractor(ISqlDialect dialect, Type commandType, Type argType, Type parameterType)
		{
			var command = Expression.Parameter(commandType, "command");
			var value = Expression.Parameter(parameterType, "value");

			var extractor = ExtractorContext.For(command, dialect);
			ExtractValues(extractor, dialect, string.Empty, value.Convert(argType));
			return Expression.Lambda(typeof(Action<,>).MakeGenericType(commandType, parameterType), extractor.GetResult(), command, value);
		}

		public static Action<TCommand, TArg> CompileExtractor<TCommand, TArg>(ISqlDialect dialect) { 
			var extractor = CreateExtractor(dialect, typeof(TCommand), typeof(TArg), typeof(TArg));
			//ExpressionDebug.WriteExpr(extractor, Console.Out);
			return (Action<TCommand, TArg>)extractor.Compile();
		}

		class ExtractorContext
		{
			readonly ISqlDialect dialect;
			readonly PropertyInfo parameterName = typeof(IDataParameter).GetProperty(nameof(IDataParameter.ParameterName));
			readonly PropertyInfo parameterValue = typeof(IDataParameter).GetProperty(nameof(IDbDataParameter.Value));

			readonly MethodInfo createParameter;
			readonly PropertyInfo getParameters;
			readonly MethodInfo addParameter;
			readonly List<Expression> extractedValues = new();

			public readonly Expression Target;

			ExtractorContext(ISqlDialect dialect, Expression target, MethodInfo createParameter, PropertyInfo getParameters, MethodInfo addParameter) {
				this.dialect = dialect;
				this.Target = target;
				this.createParameter = createParameter;
				this.getParameters = getParameters;
				this.addParameter = addParameter;
			}

			public Expression GetResult() => Expression.Block(extractedValues);

			public static ExtractorContext For(Expression target, ISqlDialect dialect) {

				var createParameter = target.Type.GetMethod(nameof(IDbCommand.CreateParameter), Type.EmptyTypes);
				var getParameters = target.Type.GetProperty(nameof(IDbCommand.Parameters), BindingFlags.DeclaredOnly | BindingFlags.Instance | BindingFlags.Public)
					?? typeof(IDbCommand).GetProperty(nameof(IDbCommand.Parameters));
				var addParameter = getParameters.PropertyType.GetMethod("Add", new[] { createParameter.ReturnType })
					?? typeof(IList).GetMethod(nameof(IList.Add));
				return new ExtractorContext(dialect, target, createParameter, getParameters, addParameter);
			}

			public (ParameterExpression, PropertyInfo) CreateParameter(string name, Type type, DbType dbType, List<Expression> block) { 
				var (createP, pValue) = dialect.CreateParameter(name, type, dbType);
				if(createP is not null) {
					var tP = Expression.Variable(createP.Type, name);
					block.Add(Expression.Assign(tP, createP));
					return (tP, pValue);
				}

				var c = CreateParameter();
				var p = Expression.Variable(c.Type, name);
				block.Add(Expression.Assign(p, CreateParameter()));
				var nameP = Expression.Assign(
					Expression.MakeMemberAccess(p, parameterName),
					Expression.Constant(p.Name));
				block.Add(nameP);
				block.Add(Expression.Assign(Expression.Property(p, typeof(IDataParameter), nameof(IDataParameter.DbType)), Expression.Constant(dbType)));
				return (p, parameterValue);
			}

			public void AddParameter(string name, Expression initP, Type type, DbType dbType) {
				var block = new List<Expression>();
				var (p, value) = CreateParameter(name, type, dbType, block);
				var ps = new List<ParameterExpression> { p };

				if(dialect.EnsureDBNull) {
					initP =	initP.Type.IsClass 
					? Expression.Coalesce(
						Expression.Convert(initP, typeof(object)),
						Expression.Constant(DBNull.Value, typeof(object)))
					: initP;
				}
				block.Add(Expression.Assign(Expression.MakeMemberAccess(p, value), initP.Convert(value.PropertyType)));
				block.Add(AddParameterItem(p));
				extractedValues.Add(Expression.Block(ps, block));
			}

			public void AddParameter(Expression createP) =>
				extractedValues.Add(AddParameterItem(createP));

			Expression CreateParameter() => Expression.Call(Target, createParameter);
			Expression GetParameterCollection() => Expression.MakeMemberAccess(Target, getParameters);
			Expression AddParameterItem(Expression p) => Expression.Call(GetParameterCollection(), addParameter, p);
		}

		static void ExtractValues(ExtractorContext extractor, ISqlDialect dialect, string prefix, Expression input) {
			var inputValues = input.Type.GetProperties()
				.Where(x => x.CanRead)
				.Concat<MemberInfo>(input.Type.GetFields());

			foreach (var value in inputValues)  {
				var name = prefix + value.Name;
				var readMember = Expression.MakeMemberAccess(input, value);

				if(dialect.TryCreateDialectSpecificParameter(name, readMember, out var dialectParameter)) {
					extractor.AddParameter(dialectParameter);
					continue;
				}

				Expression initP = null;
				var pType = readMember.Type;
				var rawType = pType;
				if (HasSqlTypeMapping(readMember.Type))
					initP = readMember;
				else if (readMember.Type.TryGetNullableTargetType(out rawType)) {
					initP = MakeParameterFromNullable(readMember, rawType, out pType);
				}
				else if (readMember.Type == typeof(Uri)) {
					initP = AsString(readMember);
					pType = rawType = typeof(string);
				} else if (TryGetDbType(readMember, out var readAsDbType)) {
					initP = readAsDbType;
					rawType = readAsDbType.Type;
				}
				
				if(initP != null)
					extractor.AddParameter(name, initP, pType, DataBossDbType.ToDbType(rawType));
				else ExtractValues(extractor, dialect, name + "_", readMember);
			}

			Expression MakeParameterFromNullable(Expression value, Type rawType, out Type pType) {
				if(dialect.SupportsNullable) {
					pType = value.Type;
					return value;
				}

				var hasValue = Expression.Convert(Expression.Property(value, "Value"), typeof(object));
				var noValue = Expression.Constant(DBNull.Value, typeof(object));
				var c =  Expression.Condition(Expression.Property(value, "HasValue"), hasValue, noValue);
				pType = rawType;
				return c;
			}
		}

		static Expression AsString(Expression readMember) =>
			Expression.Condition(
				Expression.Equal(readMember, Expression.Constant(null)),
				Expression.Constant(null, typeof(string)),
				Expression.Call(readMember, typeof(object).GetMethod("ToString")));

		static bool TryGetDbType(Expression readMember, out Expression readAsDbType) {
			var dbType = readMember.Type.SingleOrDefault<DbTypeAttribute>();
			if(dbType != null) {
				readAsDbType = Expression.Convert(readMember, dbType.Type);
				return true;
			}
			readAsDbType = null;
			return false;
		}

		public static bool HasSqlTypeMapping(Type t) => t.IsPrimitive || MappedTypes.Contains(t) || t.IsEnum;
	}
}