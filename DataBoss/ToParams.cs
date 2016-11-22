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
		static class Extractor<T>
		{
			internal static Func<object, SqlParameter[]> Invoke = CreateExtractor(typeof(T));
		}

		static readonly ConstructorInfo ItemCtor = typeof(SqlParameter).GetConstructor(new[] { typeof(string), typeof(object) });

		static Func<object, SqlParameter[]> CreateExtractor(Type type) {
			var input = Expression.Parameter(typeof(object));
			var typedInput = Expression.Parameter(type);

			return Expression.Lambda<Func<object, SqlParameter[]>>(
				Expression.Invoke(Expression.Lambda(
					Expression.NewArrayInit(
						typeof(SqlParameter),
						ExtractValues(type, typedInput)
					), typedInput),
					Expression.Convert(input, type)), input
				).Compile();
		}

		static IEnumerable<Expression> ExtractValues(Type type, Expression input) {
			return type.GetProperties()
				.Where(x => x.CanRead)
				.Cast<MemberInfo>()
				.Concat(type.GetFields())
				.Select(item => MakeItem("@" + item.Name, Expression.Convert(Expression.MakeMemberAccess(input, item), typeof(object))));
		}

		static Expression MakeItem(string name, Expression value) => 
				Expression.New(ItemCtor, Expression.Constant(name), value);

		public static SqlParameter[] Invoke<T>(T input) => Extractor<T>.Invoke(input);

		public static void AddTo<T>(SqlCommand command, T args) => command.Parameters.AddRange(Invoke(args));
	}
}