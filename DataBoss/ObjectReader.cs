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

		class FieldMap
		{
			readonly Dictionary<string, int> fields = new Dictionary<string, int>();
			Dictionary<string, FieldMap> subFields;

			public void Add(string name, int ordinal) {
				if(name.Contains('.')) {
					var parts = name.Split('.');
					var x = this;
					var n = 0;
					for(; n != parts.Length - 1; ++n)
						x = x[parts[n]];
					x.Add(parts[n], ordinal);
				}
				else fields[name] = ordinal;
			}

			public bool TryGetOrdinal(string key, out int ordinal) => fields.TryGetValue(key, out ordinal);
			public bool TryGetSubMap(string key, out FieldMap subMap) {
				if(subFields != null && subFields.TryGetValue(key, out subMap))
					return true;
				subMap = null;
				return false;
			}

			FieldMap this[string name] {
				get {
					if(subFields == null)
						subFields = new Dictionary<string, FieldMap>();
					return subFields.GetOrAdd(name, _ => new FieldMap());
				}
			}
		}

		public static Expression<Func<IDataRecord, T>> MakeConverter<T>(IDataReader reader) {
			var fieldMap = new FieldMap();
			for(var i = 0; i != reader.FieldCount; ++i)
				fieldMap.Add(reader.GetName(i), i);

			var arg0 = Expression.Parameter(typeof(IDataRecord), "x");
			return Expression.Lambda<Func<IDataRecord, T>>(MemberInit(typeof(T), fieldMap, (field, n) => ReadField(arg0, field, Expression.Constant(n))), arg0);
		}

		static Expression MemberInit(Type fieldType, FieldMap map, Func<FieldInfo, int, Expression> read) =>
			Expression.MemberInit(
				Expression.New(fieldType),
				GetFields(map, fieldType, read)
			);

		static IEnumerable<MemberAssignment> GetFields(FieldMap fieldMap, Type targetType, Func<FieldInfo, int, Expression> read) {
			return targetType
				.GetFields()
				.Where(x => !x.IsInitOnly)
				.Select(x => {
					var ordinal = 0;
					if(fieldMap.TryGetOrdinal(x.Name, out ordinal))
						return Expression.Bind(x, read(x, ordinal));

					FieldMap subField;
					if(fieldMap.TryGetSubMap(x.Name, out subField))
						return Expression.Bind(x, MemberInit(x.FieldType, subField, read));

					return null;
				}).Where(x => x != null);
		}

		static Expression ReadField(Expression arg0, FieldInfo field, Expression ordinal) {
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