using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace DataBoss.Data
{
	public class ConverterFactory
	{
		class ConverterContext
		{
			ConverterContext(ParameterExpression arg0, MethodInfo isDBNull) {
				this.Arg0 = arg0;
				this.IsDBNull = isDBNull;
			}

			public static ConverterContext Create<TRecord>() where TRecord : IDataRecord {
				var record = typeof(TRecord);
				return new ConverterContext(Expression.Parameter(record, "x"), 
					record.GetMethod(nameof(IDataRecord.IsDBNull)) ?? typeof(IDataRecord).GetMethod(nameof(IDataRecord.IsDBNull)));
			}

			public readonly ParameterExpression Arg0;
			public readonly MethodInfo IsDBNull;

			public Expression ReadField(Type fieldType, Expression ordinal) => 
				Expression.Call(Arg0, GetGetMethod(fieldType), ordinal);

			public MethodInfo GetGetMethod(Type fieldType) {
				var getterName = "Get" + MapFieldType(fieldType);
				var getter = Arg0.Type.GetMethod(getterName) ?? typeof(IDataRecord).GetMethod("Get" + MapFieldType(fieldType));
				if(getter != null)
					return getter;

				throw new NotSupportedException($"Can't read field of type: {fieldType} given {Arg0.Type}");
			}

			public Expression IsNull(Expression o) => Expression.Call(Arg0, IsDBNull, o);

			public Expression DbNullToDefault(FieldMapItem field, Expression o, Type itemType, Expression readIt) {
				if(!field.AllowDBNull)
					return readIt;
				return Expression.Condition(
					Expression.Call(Arg0, IsDBNull, o),
					Expression.Default(itemType),
					readIt);
			}
		}

		struct MemberReader
		{
			public MemberReader(int ordinal, Expression reader, Expression isNull) {
				this.Ordinal = ordinal;
				this.Read = reader;
				this.IsNull = isNull;
			}

			public readonly int Ordinal;
			public readonly Expression Read;
			public readonly Expression IsNull;

			public Expression GetReader() {
				if (IsNull == null)
					return Read;
				return Expression.Condition(
					IsNull,
					Expression.Default(Read.Type),
					Read);
			}
		}

		readonly ConverterCollection customConversions;
		readonly IConverterCache converterCache;

		public ConverterFactory(ConverterCollection customConversions) : this(customConversions, new ConcurrentConverterCache())
		{ }

		public ConverterFactory(ConverterCollection customConversions, IConverterCache converterCache) {
			this.customConversions = new ConverterCollection(customConversions);
			this.converterCache = converterCache;
		}

		public DataRecordConverter<TReader, T> GetConverter<TReader, T>(TReader reader) where TReader : IDataReader { 
			Expression dummy = null;
			return new DataRecordConverter<TReader, T>(converterCache.GetOrAdd(reader, typeof(T), (map, result) => BuildConverter(ConverterContext.Create<TReader>(), map, ref dummy, result)));
		}

		LambdaExpression BuildConverter(ConverterContext context, FieldMap map, ref Expression anyRequiredNull, Type result) => 
			Expression.Lambda(MemberInit(context, result, map, ref anyRequiredNull), context.Arg0);

		Expression MemberInit(ConverterContext context, Type fieldType, FieldMap map, ref Expression anyRequiredNull) =>
			Expression.MemberInit(
				GetCtor(context, map, ref anyRequiredNull, fieldType),
				GetMembers(context, map, ref anyRequiredNull, fieldType));

		NewExpression GetCtor(ConverterContext context, FieldMap map, ref Expression anyRequiredNull, Type fieldType) {
			var ctors = fieldType.GetConstructors()
				.Select(ctor => new { ctor, p = ctor.GetParameters() })
				.OrderByDescending(x => x.p.Length);
			foreach(var item in ctors) {
				var pn = new Expression[item.p.Length];
				if(TryMapParameters(context, map, ref anyRequiredNull, item.p, pn))
					return Expression.New(item.ctor, pn);
			}

			if(fieldType.IsValueType)
				return Expression.New(fieldType);

			throw new InvalidOperationException("No suitable constructor found for " + fieldType);
		}

		ArraySegment<MemberAssignment> GetMembers(ConverterContext context, FieldMap map, ref Expression anyRequiredNull, Type targetType) {
			var fields = targetType.GetFields().Select(x => new { x.Name, x.FieldType, Member = (MemberInfo)x });
			var props = targetType.GetProperties().Where(x => x.CanWrite).Select(x => new { x.Name, FieldType = x.PropertyType, Member = (MemberInfo)x });
			var members = fields.Concat(props).ToArray();
			var ordinals = new int[members.Length];
			var bindings = new MemberAssignment[members.Length];
			var found = 0;
			MemberReader binding;
			foreach(var x in members) {
				if(!TryReadOrInit(context, map, ref anyRequiredNull, x.FieldType, x.Name, out binding))
					continue;
				ordinals[found] = binding.Ordinal;
				bindings[found] = Expression.Bind(x.Member, binding.GetReader());
				++found;
			}
			Array.Sort(ordinals, bindings, 0, found);
			return new ArraySegment<MemberAssignment>(bindings, 0, found);
		}

		bool TryMapParameters(ConverterContext context, FieldMap map, ref Expression anyRequiredNull, ParameterInfo[] parameters, Expression[] exprs) {
			MemberReader binding;
			for(var i = 0; i != parameters.Length; ++i) {
				if(!TryReadOrInit(context, map, ref anyRequiredNull, parameters[i].ParameterType, parameters[i].Name, out binding))
					return false;
				exprs[i] = binding.GetReader();
			}
			return true;
		}
		
		bool TryReadOrInit(ConverterContext context, FieldMap map, ref Expression anyRequiredNull, Type itemType, string itemName, out MemberReader found) {
			if(itemType.TryGetNullableTargetType(out var baseType)) {
				Expression anyChildNull = null;
				if(TryReadOrInit(context, map, ref anyChildNull, baseType, itemName, out found)) {
					found = new MemberReader(found.Ordinal, Expression.Convert(found.Read, itemType), OrElse(found.IsNull, anyChildNull));
					return true;
				}
				return false;
			}

			FieldMapItem field;
			if (map.TryGetOrdinal(itemName, out field)) {
				var o = Expression.Constant(field.Ordinal);
				Expression convertedField;
				if(!TryConvertField(context.ReadField(field.FieldType, o), itemType, out convertedField) 
				&& !(field.ProviderSpecificFieldType != null && TryConvertField(context.ReadField(field.ProviderSpecificFieldType, o), itemType, out convertedField)))
					throw new InvalidOperationException($"Can't read '{itemName}' of type {itemType.Name} given {field.FieldType.Name}");

				var thisNull = context.IsNull(o);
				if (field.AllowDBNull == false)
					anyRequiredNull = OrElse(anyRequiredNull, thisNull);
				found = new MemberReader(field.Ordinal, convertedField, field.AllowDBNull ? thisNull : null);
				return true;
			}

			FieldMap subMap;
			if(map.TryGetSubMap(itemName, out subMap)) {
				found = new MemberReader(subMap.MinOrdinal, MemberInit(context, itemType, subMap, ref anyRequiredNull), null);
				return true;
			}

			found = default(MemberReader);
			return false;
		}

		static Expression OrElse(Expression left, Expression right) {
			if(left == null)
				return right;
			if(right == null)
				return left;
			return Expression.OrElse(left, right);
		}

		bool TryConvertField(Expression rawField, Type to, out Expression convertedField) {
			var from = rawField.Type;
			if (from == to) {
				convertedField = rawField;
				return true;
			}

			if (TryGetConverter(rawField, to, out convertedField))
				return true;

			return false;
		}

		bool TryGetConverter(Expression rawField, Type to, out Expression converter) {
			if(customConversions.TryGetConverter(rawField, to, out converter))
				return true;

			if (rawField.Type == typeof(object) && to == typeof(byte[])) {
				converter = Expression.Convert(rawField, to);
				return true;
			}

			return false;
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