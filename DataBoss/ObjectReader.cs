using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace DataBoss
{
	static class DictionaryExtensions
	{
		public static TValue GetOrAdd<TKey,TValue>(this IDictionary<TKey,TValue> self, TKey key, Func<TKey,TValue> valueFactory) {
			TValue found;
			if(!self.TryGetValue(key, out found)) {
				found = valueFactory(key);
				self.Add(key, found);
			}
			return found;
		}
	}

	public class ObjectReader
	{
		static readonly MethodInfo IsDBNull = typeof(IDataRecord).GetMethod("IsDBNull");

		public IEnumerable<T> Read<T>(IDataReader source) where T : new() {
			var converter = MakeConverter<T>(source).Compile();

			while(source.Read()) {
				yield return converter(source);
			}
		} 

		public static Expression<Func<IDataRecord, T>> MakeConverter<T>(IDataReader reader) {
			var arg0 = Expression.Parameter(typeof(IDataRecord), "x");
			var fieldMap = new Dictionary<string, int>();
			Dictionary<string,Dictionary<string,int>> subFields = null;
			for(var i = 0; i != reader.FieldCount; ++i) {
				var name = reader.GetName(i);
				if(name.Contains(".")){
					if(subFields == null)
						subFields = new Dictionary<string, Dictionary<string, int>>();
					var parts = name.Split('.');
					subFields.GetOrAdd(parts[0], _ => new Dictionary<string, int>())[parts[1]] = i;
				}
				else
					fieldMap[reader.GetName(i)] = i;
			}

			var targetType = typeof(T);

			var subInit = subFields == null 
				? Enumerable.Empty<MemberAssignment>()
				: subFields.Select(x => {
					var field = targetType.GetField(x.Key);
					return Expression.Bind(field, 
						Expression.MemberInit(
							Expression.New(field.FieldType.GetConstructor(Type.EmptyTypes)),
							GetFields(x.Value, field.FieldType, arg0)));
				});
			
			var init = Expression.MemberInit(
				Expression.New(targetType),
				GetFields(fieldMap, targetType, arg0).Concat(subInit));
			return Expression.Lambda<Func<IDataRecord, T>>(init, arg0);
		}

		private static IEnumerable<MemberAssignment> GetFields(IReadOnlyDictionary<string, int> fieldMap,Type targetType, Expression arg0) {
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

			return Convert(Expression.Call(arg0, GetGetMethod(arg0, fieldType), ordinal), fieldType);
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
			if(getter != null)
				return getter;

			if(fieldType == typeof(byte[])) {
				var getValue = arg0.Type.GetMethod("GetValue");
				if(getValue != null)
					return getValue;
			}

			throw new NotSupportedException("Can't read field of type:" + fieldType);
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