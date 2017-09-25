using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace DataBoss
{
	public interface IConverterCache
	{
		LambdaExpression GetOrAdd(FieldMap map, Type result, Func<FieldMap, Type, LambdaExpression> createConverter);	
	}

	class NullConverterCache : IConverterCache
	{
		NullConverterCache() { }

		public static IConverterCache Instance = new NullConverterCache();

		public LambdaExpression GetOrAdd(FieldMap map, Type result, Func<FieldMap, Type, LambdaExpression> createConverter) =>
			createConverter(map, result);
	}

	public class ConverterFactory
	{
		class DefaultConverterCache : IConverterCache
		{
			readonly Dictionary<string, LambdaExpression> converterCache = new Dictionary<string, LambdaExpression>(); 

			public LambdaExpression GetOrAdd(FieldMap map, Type result, Func<FieldMap, Type, LambdaExpression> createConverter) =>
				converterCache.GetOrAdd($"({map}) -> {result}", _ => createConverter(map, result));
		}

		readonly ParameterExpression arg0;
		readonly MethodInfo isDBNull;
		readonly ConverterCollection customConversions;
		readonly IConverterCache converterCache;

		public ConverterFactory(Type reader, ConverterCollection customConversions) : this(reader, customConversions, new DefaultConverterCache())
		{ }

		public ConverterFactory(Type reader, ConverterCollection customConversions, IConverterCache converterCache) {
			this.arg0 = Expression.Parameter(reader, "x");
			this.isDBNull = reader.GetMethod(nameof(IDataRecord.IsDBNull)) ?? typeof(IDataRecord).GetMethod(nameof(IDataRecord.IsDBNull));
			this.customConversions = new ConverterCollection(customConversions);
			this.converterCache = converterCache;
		}

		public LambdaExpression Converter(FieldMap map, Type result) =>
			converterCache.GetOrAdd(map, result, BuildConverter);

		LambdaExpression BuildConverter(FieldMap map, Type result) => 
			Expression.Lambda(MemberInit(result, map), arg0);

		Expression MemberInit(Type fieldType, FieldMap map) =>
			Expression.MemberInit(
				GetCtor(map, fieldType),
				GetMembers(map, fieldType));

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

		ArraySegment<MemberAssignment> GetMembers(FieldMap map, Type targetType) {
			var fields = targetType.GetFields().Select(x => new { x.Name, x.FieldType, Member = (MemberInfo)x });
			var props = targetType.GetProperties().Where(x => x.CanWrite).Select(x => new { x.Name, FieldType = x.PropertyType, Member = (MemberInfo)x });
			var members = fields.Concat(props).ToArray();
			var ordinals = new int[members.Length];
			var bindings = new MemberAssignment[members.Length];
			var found = 0;
			KeyValuePair<int, Expression> binding;
			foreach(var x in members) {
				if(!TryReadOrInit(map, x.FieldType, x.Name, out binding))
					continue;
				ordinals[found] = binding.Key;
				bindings[found] = Expression.Bind(x.Member, binding.Value);
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
			FieldMapItem field;
			if(map.TryGetOrdinal(itemName, out field)) {
				var o = Expression.Constant(field.Ordinal);
				Expression convertedField;
				if(!TryConvertField(ReadField(field.FieldType, o), itemType, out convertedField))
					throw new InvalidOperationException($"Can't read '{itemName}' of type {itemType.Name} given {field.FieldType.Name}");

				found = new KeyValuePair<int, Expression>(field.Ordinal, DbNullToDefault(o, itemType, convertedField));
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

		bool TryConvertField(Expression rawField, Type to, out Expression convertedField) {
			convertedField = GetConversionOrDefault(rawField, to, null);
			return convertedField != null;
		}

		Expression GetConversionOrDefault(Expression rawField, Type to, Expression defalt) {
			var from = rawField.Type;
			if(from == to) 
				return rawField;

			Type baseType = null;
			if(IsNullable(to, ref baseType)) {
				if((baseType == from))
					return Expression.Convert(rawField, to);
					else {
					var customNullabeConversion = GetConverterOrDefault(rawField, from, baseType);
					if(customNullabeConversion != null)
						return Expression.Convert(customNullabeConversion, to);
				}
			}

			if(from == typeof(object) && to == typeof(byte[]))
				return Expression.Convert(rawField, to);

			var customConversion = GetConverterOrDefault(rawField, from, to);
			if(customConversion != null) 
				return customConversion;
				
			return defalt;
		}

		Expression GetConverterOrDefault(Expression rawField, Type from, Type to) {
			if(customConversions.TryGetConverter(from, to, out var converter))
				return Expression.Invoke(Expression.Constant(converter), rawField);
			return null;
		}

		Expression DbNullToDefault(Expression o, Type itemType, Expression readIt) {
			var recordType = itemType;
			if(itemType == typeof(string) || IsNullable(itemType, ref recordType))
				return Expression.Condition(
					Expression.Call(arg0, isDBNull, o),
					Expression.Default(itemType),
					readIt);
			return readIt;
		}

		Expression ReadField(Type fieldType, Expression ordinal) => 
			Expression.Call(arg0, GetGetMethod(fieldType), ordinal);

		MethodInfo GetGetMethod(Type fieldType) {
			var getterName = "Get" + MapFieldType(fieldType);
			var getter = arg0.Type.GetMethod(getterName) ?? typeof(IDataRecord).GetMethod("Get" + MapFieldType(fieldType));
			if(getter != null)
				return getter;

			throw new NotSupportedException($"Can't read field of type: {fieldType} given {arg0.Type}");
		}

		static bool IsNullable(Type fieldType, ref Type recordType) {
			var isNullable = fieldType.TryGetNullableTargetType(out var newRecordType);
			if(isNullable)
				recordType = newRecordType;
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
}