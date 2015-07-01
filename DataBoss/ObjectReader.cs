using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace DataBoss
{
	public class ObjectReader
	{
		static readonly MethodInfo IsDBNull = typeof(IDataRecord).GetMethod("IsDBNull");

		public IEnumerable<T> Read<T>(IDataReader source) where T : new() {
			var converter = GetConverter<T>(source).Compile();

			while(source.Read()) {
				yield return converter(source);
			}
		} 

		public Expression<Func<IDataRecord, T>> GetConverter<T>(IDataReader reader) {
			var arg0 = Expression.Parameter(typeof(IDataRecord), "x");
			var fieldMap = new Dictionary<string, int>();
			for(var i = 0; i != reader.FieldCount; ++i)
				fieldMap[reader.GetName(i)] = i;

			var targetType = typeof(T);
			var init = Expression.MemberInit(
				Expression.New(targetType.GetConstructor(Type.EmptyTypes)), GetFields(fieldMap, targetType, arg0));
			return Expression.Lambda<Func<IDataRecord, T>>(init, arg0);
		}

		private static IEnumerable<MemberAssignment> GetFields(IReadOnlyDictionary<string, int> fieldMap,Type targetType, ParameterExpression arg0) {
			var dummy = 0;
			return targetType
				.GetFields()
				.Where(x => !x.IsInitOnly)
				.Where(x => fieldMap.TryGetValue(x.Name, out dummy))
				.Select(field => new { field, ordinal = Expression.Constant(dummy)})
				.Select(x => Expression.Bind(x.field, ReadField(arg0, x.field, x.ordinal)));
		}

		private static Expression ReadField(Expression arg0, FieldInfo field, Expression ordinal) {
			var fieldType = field.FieldType;
			var recordType = fieldType;
			if (fieldType == typeof(string) || IsNullable(fieldType, ref recordType))
				return Expression.Condition(
					Expression.Call(arg0, IsDBNull, ordinal),
					Expression.Default(fieldType),
					Convert(Expression.Call(arg0, GetGetMethod(arg0, recordType), ordinal), fieldType));

			return Expression.Call(arg0, GetGetMethod(arg0, fieldType), ordinal);
		}

		static Expression Convert(Expression expr, Type targetType) {
			return expr.Type == targetType
				? expr
				: Expression.Convert(expr, targetType);
		} 

		private static bool IsNullable(Type fieldType, ref Type recordType) {
			var isNullable = fieldType.IsGenericType && fieldType.GetGenericTypeDefinition() == typeof(Nullable<>);
			if(isNullable)
				recordType = fieldType.GetGenericArguments()[0];
			return isNullable;
		}

		private static MethodInfo GetGetMethod(Expression arg0, Type fieldType) {
			var getter = arg0.Type.GetMethod("Get" + MapFieldType(fieldType));
			if(getter == null)
				throw new NotSupportedException("Can't read field of type:" + fieldType);
			return getter;
		}

		private static string MapFieldType(Type fieldType) {
			switch(fieldType.FullName) {
				case "System.Single": return "Float";
				case "System.Object": return "Value";
			}
			return fieldType.Name;
		}
	}
}