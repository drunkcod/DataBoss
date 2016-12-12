using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace DataBoss
{
	public static class ToParams
	{
		static HashSet<Type> mappedTypes = new HashSet<Type>
		{
			typeof(string),
			typeof(DateTime),
			typeof(Decimal),
			typeof(byte[]),
		};

		static class Extractor<T>
		{
			internal static Func<T, SqlParameter[]> GetParameters = (Func<T, SqlParameter[]>)CreateExtractor(typeof(T)).Compile();
		}

		static readonly ConstructorInfo SqlParameterCtor = typeof(SqlParameter).GetConstructor(new[] { typeof(string), typeof(object) });

		static LambdaExpression CreateExtractor(Type type) {
			var typedInput = Expression.Parameter(type);

			return Expression.Lambda(
				Expression.NewArrayInit(
					typeof(SqlParameter),
					ExtractValues(type, "@", typedInput)
				), typedInput);
		}

		static IEnumerable<Expression> ExtractValues(Type type, string prefix, Expression input) {
			foreach(var value in type.GetProperties()
				.Where(x => x.CanRead)
				.Select(x => new { Member = x as MemberInfo, Type = x.PropertyType })
				.Concat(type.GetFields().Select(x => new { Member = x as MemberInfo, Type = x.FieldType }))
			) {
				var name = prefix + value.Member.Name;
				var readMember = Expression.MakeMemberAccess(input, value.Member);
				if(HasSqlTypeMapping(value.Type))
					yield return MakeSqlParameter(name, readMember);
				else
					foreach(var item in ExtractValues(value.Type, name + "_", readMember))
						yield return item;
			}
		}

		public static bool HasSqlTypeMapping(Type t) => t.IsPrimitive || mappedTypes.Contains(t);

		static Expression MakeSqlParameter(string name, Expression value) => 
			Expression.New(SqlParameterCtor, Expression.Constant(name), Expression.Convert(value, typeof(object)));

		public static SqlParameter[] Invoke<T>(T input) => Extractor<T>.GetParameters(input);

		public static void AddTo<T>(SqlCommand command, T args) => command.Parameters.AddRange(Invoke(args));
	}
}