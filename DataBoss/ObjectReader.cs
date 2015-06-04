using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Linq.Expressions;

namespace DataBoss
{
	public class ObjectReader
	{
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
				.Select(x => Expression.Bind(x.field, Expression.Call(arg0, arg0.Type.GetMethod("Get" + x.field.FieldType.Name), x.ordinal)));
		}
	}
}