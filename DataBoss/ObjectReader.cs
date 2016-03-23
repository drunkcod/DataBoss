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

		public IEnumerable<T> Read<T>(IDataReader source) {
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

		static Expression MemberInit(Type fieldType, FieldMap map, Func<Type, int, Expression> read) =>
			Expression.MemberInit(
				GetCtor(map, fieldType, read),
				GetFields(map, fieldType, read)
			);

		static NewExpression GetCtor(FieldMap map, Type fieldType, Func<Type, int, Expression> read)
		{
			var ctors = fieldType.GetConstructors()
				.Select(ctor => new { ctor, p = ctor.GetParameters() })
				.OrderByDescending(x => x.p.Length);
			foreach(var item in ctors) {
				var pn = new Expression[item.p.Length];
				if(TryMaParameters(map, item.p, read, pn))
					return Expression.New(item.ctor, pn);
			}

			return Expression.New(fieldType);
		}

		static bool TryMaParameters(FieldMap map, ParameterInfo[] parameters, Func<Type, int, Expression> read, Expression[] exprs) {
			int ordinal;
			for(var i = 0; i != parameters.Length; ++i)
				if(!map.TryGetOrdinal(parameters[i].Name, out ordinal))
					return false;
				else exprs[i] = read(parameters[i].ParameterType, ordinal);
			return true;
		} 

		static IEnumerable<MemberAssignment> GetFields(FieldMap fieldMap, Type targetType, Func<Type, int, Expression> read) {
			return targetType
				.GetFields()
				.Where(x => !x.IsInitOnly)
				.Select(x => {
					var ordinal = 0;
					if(fieldMap.TryGetOrdinal(x.Name, out ordinal))
						return Expression.Bind(x, read(x.FieldType, ordinal));

					FieldMap subField;
					if(fieldMap.TryGetSubMap(x.Name, out subField))
						return Expression.Bind(x, MemberInit(x.FieldType, subField, read));

					return null;
				}).Where(x => x != null);
		}

		static Expression ReadField(Expression arg0, Type fieldType, Expression ordinal) {
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