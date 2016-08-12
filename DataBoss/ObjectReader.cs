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
			var converter = GetConverter<T>(source);
			while(source.Read()) {
				yield return converter(source);
			}
		} 

		class FieldMap
		{
			readonly Dictionary<string, int> fields = new Dictionary<string, int>();
			Dictionary<string, FieldMap> subFields;

			public int MinOrdinal => fields.Count == 0 ? -1 : fields.Min(x => x.Value);

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

		class ConverterFactory
		{
			readonly ParameterExpression arg0 = Expression.Parameter(typeof(IDataRecord), "x");

			Expression ReadField(Type fieldType, int ordinal) {
				var recordType = fieldType;
				var o = Expression.Constant(ordinal);
				if (fieldType == typeof(string) || IsNullable(fieldType, ref recordType))
					return Expression.Condition(
						Expression.Call(arg0, IsDBNull, o),
						Expression.Default(fieldType),
						ReadFieldAs(recordType, o, fieldType));

				return ReadFieldAs(fieldType, o, fieldType);
			}

			Expression ReadFieldAs(Type fieldType, Expression ordinal, Type targetType) => 
				Convert(Expression.Call(arg0, GetGetMethod(arg0, fieldType), ordinal), targetType);

			public Expression<Func<IDataRecord,T>> Converter<T>(FieldMap map) => 
				Expression.Lambda<Func<IDataRecord,T>>(MemberInit(typeof(T), map), arg0);

			static Expression Convert(Expression expr, Type targetType) =>
				expr.Type == targetType
				? expr
				: Expression.Convert(expr, targetType);
			
			Expression MemberInit(Type fieldType, FieldMap map) =>
				Expression.MemberInit(
					GetCtor(map, fieldType),
					GetFields(map, fieldType));

			NewExpression GetCtor(FieldMap map, Type fieldType) {
				var ctors = fieldType.GetConstructors()
					.Select(ctor => new { ctor, p = ctor.GetParameters() })
					.OrderByDescending(x => x.p.Length);
				foreach(var item in ctors) {
					var pn = new Expression[item.p.Length];
					if(TryMaParameters(map, item.p, pn))
						return Expression.New(item.ctor, pn);
				}

				if(fieldType.IsValueType)
					return Expression.New(fieldType);

				throw new InvalidOperationException("No suitable constructor found for " + fieldType);
			}

			bool TryMaParameters(FieldMap map, ParameterInfo[] parameters, Expression[] exprs) {
				int ordinal; 
				FieldMap subMap;
				for(var i = 0; i != parameters.Length; ++i)
					if(map.TryGetOrdinal(parameters[i].Name, out ordinal))
						exprs[i] = ReadField(parameters[i].ParameterType, ordinal);
					else if(map.TryGetSubMap(parameters[i].Name, out subMap)) {
						exprs[i] = MemberInit(parameters[i].ParameterType, subMap);
					}
					else return false;
				return true;
			} 

			IEnumerable<MemberAssignment> GetFields(FieldMap fieldMap, Type targetType) {
				var fields = targetType.GetFields();
				var ordinals = new int[fields.Length];
				var bindings = new MemberAssignment[fields.Length];
				var found = 0;
				FieldMap subField;
				foreach(var x in fields) {
					if(fieldMap.TryGetOrdinal(x.Name, out ordinals[found])) {
						bindings[found] = Expression.Bind(x, ReadField(x.FieldType, ordinals[found]));
						++found;
					}
					else if(fieldMap.TryGetSubMap(x.Name, out subField)) {
						ordinals[found] = subField.MinOrdinal;
						bindings[found++] = Expression.Bind(x, MemberInit(x.FieldType, subField));
					}
				}
				Array.Sort(ordinals, bindings, 0, found);
				return new ArraySegment<MemberAssignment>(bindings, 0, found);
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

				throw new NotSupportedException("Can't read field of type:" + fieldType);
			}

			private static string MapFieldType(Type fieldType) {
				switch(fieldType.FullName) {
					case "System.Single": return "Float";
					case "System.Object": return "Value";
					case "System.Byte[]": return "Value";
				}
				return fieldType.Name;
			}
		}

		public static Func<IDataRecord,T> GetConverter<T>(IDataReader reader) => 
			MakeConverter<T>(reader).Compile();

		public static Expression<Func<IDataRecord, T>> MakeConverter<T>(IDataReader reader) {
			var fieldMap = new FieldMap();
			for(var i = 0; i != reader.FieldCount; ++i)
				fieldMap.Add(reader.GetName(i), i);
			var rr = new ConverterFactory();
			return rr.Converter<T>(fieldMap);
		}
	}
}