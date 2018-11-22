using System;
using System.ComponentModel.DataAnnotations;
using System.Data;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace DataBoss.Data
{
	public class InvalidConversionException : InvalidOperationException
	{
		public InvalidConversionException(string message, Type type) :base(message) { 
			this.Type = type;
		}

		public readonly Type Type;

		public override string Message => $"Error reading {Type}: " + base.Message;
	}

	public class ConverterFactory
	{
		class ConverterContext
		{
			ConverterContext(ParameterExpression arg0, MethodInfo isDBNull, Type resultType) {
				this.Arg0 = arg0;
				this.IsDBNull = isDBNull;
				this.ResultType = resultType;
			}

			public readonly Type ResultType;

			public static ConverterContext Create(Type recordType, Type resultType) {
				return new ConverterContext(Expression.Parameter(recordType, "x"), 
					recordType.GetMethod(nameof(IDataRecord.IsDBNull)) ?? typeof(IDataRecord).GetMethod(nameof(IDataRecord.IsDBNull)),
					resultType);
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

				throw new InvalidConversionException($"Can't read field of type: {fieldType} given {Arg0.Type}", ResultType);
			}

			public Expression IsNull(Expression o) => Expression.Call(Arg0, IsDBNull, o);

			public Expression DbNullToDefault(FieldMapItem field, Expression o, Type itemType, Expression readIt) {
				if(!field.CanBeNull)
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

		struct ChildBinding
		{
			public Expression AnyRequiredNull;
			public MemberReader Reader;
			public bool IsRequired;

			public Expression IsNull => OrElse(AnyRequiredNull, Reader.IsNull);

			public MemberReader GetMemberReader(Type itemType) => 
				new MemberReader(Reader.Ordinal, ReadAs(itemType), IsNull);

			Expression ReadAs(Type itemType) => Expression.Convert(Reader.Read, itemType);
		}

		readonly DataRecordConverterFactory recordConverterFactory;
		readonly IConverterCache converterCache;

		public ConverterFactory(ConverterCollection customConversions) : this(customConversions, new ConcurrentConverterCache())
		{ }

		public ConverterFactory(ConverterCollection customConversions, IConverterCache converterCache) {
			this.recordConverterFactory = new DataRecordConverterFactory(new ConverterCollection(customConversions));
			this.converterCache = converterCache;
		}

		class DataRecordConverterFactory
		{
			readonly ConverterCollection customConversions;

			public DataRecordConverterFactory(ConverterCollection customConversions) { this.customConversions = customConversions; }

			public DataRecordConverter BuildConverter(Type readerType, FieldMap map, Type result) {
				var root = new ChildBinding();
				return BuildConverter(ConverterContext.Create(readerType, result), map, result, ref root);
			}

			public DataRecordConverter BuildConverter<TReader,TArg0, T>(FieldMap map, Expression<Func<TArg0, T>> factory) where TReader : IDataReader {
				var context = ConverterContext.Create(typeof(TReader), typeof(T));
				var pn = new Expression[factory.Parameters.Count];
				var root = new ChildBinding();
				if (TryMapParameters(context, map, ref root, factory.Parameters.Select(x => (x.Type, x.Name)).ToArray(), pn))
					return new DataRecordConverter(Expression.Lambda(Expression.Invoke(factory, pn), context.Arg0));

				throw new InvalidConversionException("Mapping failed.", typeof(T));
			}

			DataRecordConverter BuildConverter(ConverterContext context, FieldMap map, Type result, ref ChildBinding item) =>
				new DataRecordConverter(Expression.Lambda(MemberInit(context, result, map, ref item), context.Arg0));

			Expression MemberInit(ConverterContext context, Type fieldType, FieldMap map, ref ChildBinding item) =>
				Expression.MemberInit(
					GetCtor(context, map, fieldType, ref item),
					GetMembers(context, map, fieldType, ref item));

			NewExpression GetCtor(ConverterContext context, FieldMap map, Type fieldType, ref ChildBinding item) {
				var ctors = fieldType.GetConstructors()
					.Select(ctor => new { ctor, p = Array.ConvertAll(ctor.GetParameters(), x => (x.ParameterType, x.Name)) })
					.OrderByDescending(x => x.p.Length);
				foreach (var x in ctors) {
					var pn = new Expression[x.p.Length];
					if (TryMapParameters(context, map, ref item, x.p, pn))
						return Expression.New(x.ctor, pn);
				}

				if (fieldType.IsValueType)
					return Expression.New(fieldType);

				throw new InvalidConversionException("No suitable constructor found for " + fieldType, context.ResultType);
			}

			ArraySegment<MemberAssignment> GetMembers(ConverterContext context, FieldMap map, Type targetType, ref ChildBinding item) {
				var fields = targetType.GetFields().Where(x => !x.IsInitOnly).Select(x => new { x.Name, x.FieldType, Member = (MemberInfo)x });
				var props = targetType.GetProperties().Where(x => x.CanWrite).Select(x => new { x.Name, FieldType = x.PropertyType, Member = (MemberInfo)x });
				var members = fields.Concat(props).ToArray();
				var ordinals = new int[members.Length];
				var bindings = new MemberAssignment[members.Length];
				var found = 0;
				foreach (var x in members) {
					if (!TryReadOrInit(context, map, x.FieldType, x.Name, ref item))
						if (x.Member.GetCustomAttributes(typeof(RequiredAttribute), false).Length != 0)
							throw new ArgumentException("Failed to set required member.", x.Name);
						else continue;
					ordinals[found] = item.Reader.Ordinal;
					bindings[found] = Expression.Bind(x.Member, item.Reader.GetReader());
					++found;
				}
				Array.Sort(ordinals, bindings, 0, found);
				return new ArraySegment<MemberAssignment>(bindings, 0, found);
			}

			bool TryMapParameters(ConverterContext context, FieldMap map, ref ChildBinding item, (Type ParameterType, string Name)[] parameters, Expression[] exprs) {
				for (var i = 0; i != parameters.Length; ++i) {
					if (!TryReadOrInit(context, map, parameters[i].ParameterType, parameters[i].Name, ref item))
						return false;
					exprs[i] = item.Reader.GetReader();
				}
				return true;
			}

			bool TryReadOrInit(ConverterContext context, FieldMap map, Type itemType, string itemName, ref ChildBinding item) {
				if (itemType.TryGetNullableTargetType(out var baseType)) {
					var childItem = new ChildBinding { IsRequired = true };
					if (TryReadOrInit(context, map, baseType, itemName, ref childItem)) {
						item.Reader = childItem.GetMemberReader(itemType);
						return true;
					}
					return false;
				}

				FieldMapItem column;
				if (map.TryGetOrdinal(itemName, out column)) {
					var o = Expression.Constant(column.Ordinal);
					Expression convertedField;
					if (!TryConvertField(context.ReadField(column.FieldType, o), itemType, out convertedField)
					&& !(column.ProviderSpecificFieldType != null && TryConvertField(context.ReadField(column.ProviderSpecificFieldType, o), itemType, out convertedField)))
						throw new InvalidConversionException($"Can't read '{itemName}' of type {itemType.Name} given {column.FieldType.Name}", context.ResultType);

					var thisNull = context.IsNull(o);
					if (item.IsRequired && column.CanBeNull)
						item.AnyRequiredNull = OrElse(item.AnyRequiredNull, thisNull);
					item.Reader = new MemberReader(column.Ordinal, convertedField, column.CanBeNull ? thisNull : null);
					return true;
				}

				FieldMap subMap;
				if (map.TryGetSubMap(itemName, out subMap)) {
					item.Reader = new MemberReader(subMap.MinOrdinal, MemberInit(context, itemType, subMap, ref item), null);
					return true;
				}

				return false;
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
				if (customConversions.TryGetConverter(rawField, to, out converter))
					return true;

				if (rawField.Type == typeof(object) && to == typeof(byte[])) {
					converter = Expression.Convert(rawField, to);
					return true;
				}

				return false;
			}
		}

		public DataRecordConverter<TReader, T> GetConverter<TReader, T>(TReader reader) where TReader : IDataReader =>
			converterCache.GetOrAdd(
				reader, ConverterCacheKey.Create(reader, typeof(T)),
				x => recordConverterFactory.BuildConverter(typeof(TReader), x, typeof(T)))
			.ToTyped<TReader, T>();
	
		public DataRecordConverter<TReader, T> GetConverter<TReader, TArg0, T>(TReader reader, Expression<Func<TArg0, T>> factory) where TReader : IDataReader {
			if(ConverterCacheKey.TryCreate(typeof(TReader), factory, out var key)) {
				return converterCache.GetOrAdd(
					reader, key,
					x => recordConverterFactory.BuildConverter<TReader, TArg0, T>(x, factory))
				.ToTyped<TReader, T>();
			}

			return recordConverterFactory
				.BuildConverter<TReader, TArg0, T>(FieldMap.Create(reader), factory)
				.ToTyped<TReader, T>();
		}

		static Expression OrElse(Expression left, Expression right) {
			if(left == null)
				return right;
			if(right == null)
				return left;
			return Expression.OrElse(left, right);
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