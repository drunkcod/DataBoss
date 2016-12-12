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
		public static ObjectReader<TReader> For<TReader>(TReader reader) where TReader : IDataReader =>
			new ObjectReader<TReader>(reader); 

		class FieldMap
		{
			readonly Dictionary<string, int> fields = new Dictionary<string, int>();
			Dictionary<string, FieldMap> subFields;

			public static FieldMap Create(IDataRecord reader) {
				var fieldMap = new FieldMap();
				for(var i = 0; i != reader.FieldCount; ++i)
					fieldMap.Add(reader.GetName(i), i);
				return fieldMap;
			}

			public int MinOrdinal => fields.Count == 0 ? -1 : fields.Min(x => x.Value);

			public void Add(string name, int ordinal) {
				if(name.Contains('.')) {
					var parts = name.Split('.');
					var x = this;
					for(var n = 0; n != parts.Length - 1; ++n)
						x = x[parts[n]];
					x.Add(parts[parts.Length - 1], ordinal);
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
			readonly ParameterExpression arg0;
			readonly MethodInfo isDBNull;

			public ConverterFactory(Type reader) {
				this.arg0 = Expression.Parameter(reader, "x");
				this.isDBNull = reader.GetMethod("IsDBNull");
			}

			public LambdaExpression Converter(FieldMap map, Type source) =>
				Expression.Lambda(MemberInit(source, map), arg0);

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
					if(TryMapParameters(map, item.p, pn))
						return Expression.New(item.ctor, pn);
				}

				if(fieldType.IsValueType)
					return Expression.New(fieldType);

				throw new InvalidOperationException("No suitable constructor found for " + fieldType);
			}

			ArraySegment<MemberAssignment> GetFields(FieldMap map, Type targetType) {
				var fields = targetType.GetFields();
				var ordinals = new int[fields.Length];
				var bindings = new MemberAssignment[fields.Length];
				var found = 0;
				KeyValuePair<int, Expression> binding;
				foreach(var x in fields) {
					if(!TryReadOrInit(map, x.FieldType, x.Name, out binding))
						continue;
					ordinals[found] = binding.Key;
					bindings[found] = Expression.Bind(x, binding.Value);
					++found;
				}
				Array.Sort(ordinals, bindings, 0, found);
				return new ArraySegment<MemberAssignment>(bindings, 0, found);
			}

			bool TryMapParameters(FieldMap map, ParameterInfo[] parameters, Expression[] exprs) {
				KeyValuePair<int, Expression> binding;
				for(var i = 0; i != parameters.Length; ++i) {
					if(!TryReadOrInit(map, parameters[i].ParameterType, parameters[i].Name, out binding))
						return false;
					exprs[i] = binding.Value;
				}
				return true;
			}

			bool TryReadOrInit(FieldMap map, Type itemType, string itemName, out KeyValuePair<int, Expression> found) {
				int ordinal;
				if(map.TryGetOrdinal(itemName, out ordinal)) {
					found = new KeyValuePair<int, Expression>(ordinal, ReadField(itemType, ordinal));
					return true;
				}

				FieldMap subMap;
				if(map.TryGetSubMap(itemName, out subMap)) {
					found = new KeyValuePair<int, Expression>(subMap.MinOrdinal, MemberInit(itemType, subMap));
					return true;
				}

				found = default(KeyValuePair<int, Expression>);
				return false;
			}

			Expression ReadField(Type fieldType, int ordinal) {
				var recordType = fieldType;
				var o = Expression.Constant(ordinal);
				if (fieldType == typeof(string) || IsNullable(fieldType, ref recordType))
					return Expression.Condition(
						Expression.Call(arg0, isDBNull, o),
						Expression.Default(fieldType),
						ReadFieldAs(recordType, o, fieldType));

				return ReadFieldAs(fieldType, o, fieldType);
			}

			Expression ReadFieldAs(Type fieldType, Expression ordinal, Type targetType) => 
				Convert(Expression.Call(arg0, GetGetMethod(fieldType), ordinal), targetType);

			MethodInfo GetGetMethod(Type fieldType) {
				var getter = arg0.Type.GetMethod("Get" + MapFieldType(fieldType));
				if(getter != null)
					return getter;

				throw new NotSupportedException($"Can't read field of type: {fieldType} given {arg0.Type}");
			}

			static Expression Convert(Expression expr, Type targetType) =>
				expr.Type == targetType
				? expr
				: Expression.Convert(expr, targetType);
			
			static bool IsNullable(Type fieldType, ref Type recordType) {
				var isNullable = fieldType.IsGenericType && fieldType.GetGenericTypeDefinition() == typeof(Nullable<>);
				if(isNullable)
					recordType = fieldType.GetGenericArguments()[0];
				return isNullable;
			}

			static string MapFieldType(Type fieldType) {
				switch(fieldType.FullName) {
					case "System.Single": return "Float";
					case "System.Object": return "Value";
					case "System.Byte[]": return "Value";
				}
				return fieldType.Name;
			}
		}

		public static Func<TReader, T> GetConverter<TReader, T>(TReader reader) where TReader : IDataRecord => 
			MakeConverter<TReader, T>(reader).Compile();

		public static Expression<Func<TReader, T>> MakeConverter<TReader, T>(TReader reader) where TReader : IDataRecord =>
			(Expression<Func<TReader, T>>)new ConverterFactory(typeof(TReader)).Converter(FieldMap.Create(reader), typeof(T));
	}

	public struct ObjectReader<TReader> : IDisposable where TReader : IDataReader
	{
		readonly TReader reader;
		public ObjectReader(TReader reader) { this.reader = reader; }

		void IDisposable.Dispose() => reader.Dispose();

		public Func<TReader, T> GetConverter<T>() => ObjectReader.GetConverter<TReader, T>(reader);

		public IEnumerable<T> Read<T>() {
			var converter = GetConverter<T>();
			while(reader.Read())
				yield return converter(reader);
		}

		public void Read<T>(Action<T> handleItem) {
			var converter = GetConverter<T>();
			while(reader.Read())
				handleItem(converter(reader));
		}

		public bool NextResult() => reader.NextResult();
	}
}