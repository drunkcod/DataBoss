using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Data;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using DataBoss.Data.SqlServer;

namespace DataBoss.Data
{
	public class ConverterFactory
	{
		class ConverterContext
		{
			readonly ConverterCollection converters;

			ConverterContext(ParameterExpression arg0, MethodInfo isDBNull, Type resultType, ConverterCollection converters) {
				this.Arg0 = arg0;
				this.IsDBNull = isDBNull;
				this.ResultType = resultType;
				this.converters = converters;
			}

			public readonly Type ResultType;
			public readonly ParameterExpression Arg0;
			public readonly MethodInfo IsDBNull;

			public static ConverterContext Create(Type recordType, Type resultType, ConverterCollection converters) {
				return new ConverterContext(Expression.Parameter(recordType, "x"), 
					recordType.GetMethod(nameof(IDataRecord.IsDBNull)) ?? typeof(IDataRecord).GetMethod(nameof(IDataRecord.IsDBNull)),
					resultType,
					converters);
			}

			public bool TryReadFieldAs(Type fieldType, Expression ordinal, Type itemType, out Expression reader) {
				if(TryReadField(fieldType, ordinal, out var readRaw) 
				&& TryConvertField(readRaw, itemType, out reader))
					return true;

				reader = null;
				return false;
			}

			public bool TryReadField(Type fieldType, Expression ordinal, out Expression reader) {  
				if(TryGetGetMethod(fieldType, out var getter)) {
					reader = Expression.Call(Arg0, getter, ordinal);
					return true;
				}

				reader = null;
				return false;
			}

			bool TryGetGetMethod(Type fieldType, out MethodInfo getter) {
				var getterName = "Get" + MapFieldType(fieldType);
				getter = Arg0.Type.GetMethod(getterName) ?? typeof(IDataRecord).GetMethod(getterName);
				return getter != null;
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
				if (converters.TryGetConverter(rawField, to, out converter))
					return true;

				if (IsByteArray(rawField, to) || IsIdOf(rawField, to) || IsEnum(rawField, to) || HasExplictCast(rawField, to)) {
					converter = Expression.Convert(rawField, to);
					return true;
				}

				return false;
			}

			static bool IsByteArray(Expression rawField, Type to) =>
				rawField.Type == typeof(object) && to == typeof(byte[]);

			static bool IsIdOf(Expression rawField, Type to) =>
				(to.IsGenericType && to.GetGenericTypeDefinition() == typeof(IdOf<>) && rawField.Type == typeof(int));

			static bool IsEnum(Expression rawField, Type to) =>
				to.IsEnum && Enum.GetUnderlyingType(to) == rawField.Type;

			static bool HasExplictCast(Expression rawField, Type to) {
				var cast = to.GetMethod("op_Explicit", new[] { rawField.Type });

				return cast != null && to.IsAssignableFrom(cast.ReturnType);
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

			public BindingResult TryReadOrInit(FieldMap map, Type itemType, string itemName, ref ChildBinding item, out MemberReader reader, bool throwOnConversionFailure) {
				if (itemType.TryGetNullableTargetType(out var baseType)) {
					//IsRequired since otherwise we'd get a default(T) and thus a nullable with a value.
					var childItem = new ChildBinding { IsRequired = true };
					var r = TryReadOrInit(map, baseType, itemName, ref childItem, out var childReader, throwOnConversionFailure: true);
					if (r == BindingResult.Ok) {
						reader = new MemberReader(
							childReader.Ordinal, 
							Expression.Convert(childReader.Read, itemType), 
							OrElse(childReader.IsNull, childItem.AnyRequiredNull));
						return BindingResult.Ok;;
					}
					reader = default;
					return r;
				}

				if (map.TryGetField(itemName, out var field)) {
					var o = Expression.Constant(field.Ordinal);
					Expression convertedField;
					var canReadAsItem =
						TryReadFieldAs(field.FieldType, o, itemType, out convertedField)
						|| (field.ProviderSpecificFieldType != null && TryReadFieldAs(field.ProviderSpecificFieldType, o, itemType, out convertedField));
					if (!canReadAsItem) {
						reader = default;
						return throwOnConversionFailure
							? throw InvalidConversion(map, itemType, itemName)
							: BindingResult.InvalidCast;
					}

					var thisNull = field.CanBeNull ? IsNull(o) : null;
					if (item.IsRequired && thisNull != null)
						item.AnyRequiredNull = OrElse(item.AnyRequiredNull, thisNull);
					reader = new MemberReader(field.Ordinal, convertedField, thisNull);
					return BindingResult.Ok;
				}

				if (map.TryGetSubMap(itemName, out var subMap)) {
					reader = new MemberReader(subMap.MinOrdinal, FieldInit(subMap, itemType, ref item), null);
					return BindingResult.Ok;
				}
				reader = default;
				return BindingResult.NotFound;
			}

			InvalidConversionException InvalidConversion(FieldMap map, Type itemType, string itemName) {
				map.TryGetField(itemName, out var field);
				return new InvalidConversionException($"Can't read '{itemName}' of type {itemType.Name} given {field.FieldType.Name}", ResultType);
			}

			public Expression FieldInit(FieldMap map, Type fieldType, ref ChildBinding item) {
				var ctor = GetCtor(map, fieldType, ref item);

				if (ctor == null) {
					var fun = GetFactoryFunction(map, fieldType, ref item);
					if (fun != null)
						return fun;
					if (fieldType.IsValueType)
						ctor = Expression.New(fieldType);
					else throw new InvalidConversionException("No suitable constructor found for " + fieldType, ResultType);
				}

				return Expression.MemberInit(ctor,
					GetMembers(map, fieldType, ref item));
			}

			NewExpression GetCtor(FieldMap map, Type fieldType, ref ChildBinding item) {
				var ctors = fieldType.GetConstructors()
					.Select(ctor => (ctor, p: Array.ConvertAll(ctor.GetParameters(), x => (x.ParameterType, x.Name))))
					.OrderByDescending(x => x.p.Length);

				foreach (var x in ctors) {
					var pn = new Expression[x.p.Length];
					if (TryMapParameters(map, ref item, x.p, pn, throwOnConversionFailure: false))
						return Expression.New(x.ctor, pn);
				}

				return null;
			}

			Expression GetFactoryFunction(FieldMap map, Type fieldType, ref ChildBinding item) {
				var factoryFuns = fieldType
					.GetMethods(BindingFlags.Static | BindingFlags.Public)
					.Where(x => x.GetCustomAttribute(typeof(ConsiderAsCtorAttribute)) != null)
					.Select(f => (fun: f, p: Array.ConvertAll(f.GetParameters(), x => (x.ParameterType, x.Name))))
					.OrderByDescending(x => x.p.Length);

				foreach (var x in factoryFuns) {
					var pn = new Expression[x.p.Length];
					if (TryMapParameters(map, ref item, x.p, pn, throwOnConversionFailure: false))
						return Expression.Call(x.fun, pn);
				}

				return null;
			}

			ArraySegment<MemberAssignment> GetMembers(FieldMap map, Type targetType, ref ChildBinding item) {
				var fields = targetType.GetFields().Where(x => !x.IsInitOnly).Select(x => (x.Name, x.FieldType, Member: (MemberInfo)x));
				var props = targetType.GetProperties().Where(x => x.CanWrite).Select(x => (x.Name, FieldType: x.PropertyType, Member: (MemberInfo)x));
				var members = fields.Concat(props).ToArray();
				var ordinals = new int[members.Length];
				var bindings = new MemberAssignment[members.Length];
				var found = 0;
				foreach (var x in members) {
					switch(TryReadOrInit(map, x.FieldType, x.Name, ref item, out var reader, throwOnConversionFailure: false)) {
						case BindingResult.InvalidCast: throw InvalidConversion(map, x.FieldType, x.Name);
						case BindingResult.NotFound:
							if (x.Member.GetCustomAttribute(typeof(RequiredAttribute), false) != null)
								throw new ArgumentException("Failed to set required member.", x.Name);
							else continue;

						case BindingResult.Ok:
							ordinals[found] = reader.Ordinal;
							bindings[found] = Expression.Bind(x.Member, reader.GetReader());
							++found;
							break;
					}
				}
				Array.Sort(ordinals, bindings, 0, found);
				return new ArraySegment<MemberAssignment>(bindings, 0, found);
			}

			bool TryMapParameters(FieldMap map, ref ChildBinding item, (Type ParameterType, string Name)[] parameters, Expression[] exprs, bool throwOnConversionFailure) {
				for (var i = 0; i != parameters.Length; ++i) {
					if (TryReadOrInit(map, parameters[i].ParameterType, parameters[i].Name, ref item, out var reader, throwOnConversionFailure) != BindingResult.Ok)
						return false;
					exprs[i] = reader.GetReader();
				}
				return true;
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
			public bool IsRequired;
		}

		enum BindingResult
		{
			Ok,
			NotFound,
			InvalidCast
		}

		readonly DataRecordConverterFactory recordConverterFactory;
		readonly IConverterCache converterCache;

		public ConverterFactory(ConverterCollection customConversions) : this(customConversions, new ConcurrentConverterCache())
		{ }

		public ConverterFactory(ConverterCollection customConversions, IConverterCache converterCache) {
			this.recordConverterFactory = new DataRecordConverterFactory(new ConverterCollection(customConversions));
			this.converterCache = converterCache;
		}

		public static ConverterFactory Default = new ConverterFactory(null, NullConverterCache.Instance);

		class DataRecordConverterFactory
		{
			readonly ConverterCollection customConversions;

			public DataRecordConverterFactory(ConverterCollection customConversions) { this.customConversions = customConversions; }

			public DataRecordConverter BuildConverter(Type readerType, FieldMap map, Type result) {
				var root = new ChildBinding();
				var context = ConverterContext.Create(readerType, result, customConversions);
				return new DataRecordConverter(Expression.Lambda(context.FieldInit(map, context.ResultType, ref root), context.Arg0));
			}

			public DataRecordConverter BuildConverter<TReader>(FieldMap map, LambdaExpression resultFactory) where TReader : IDataReader {
				var context = ConverterContext.Create(typeof(TReader), resultFactory.Type, customConversions);
				var pn = BindAllParameters(context, map, resultFactory.Parameters);
				return new DataRecordConverter(Expression.Lambda(Expression.Invoke(resultFactory, pn), context.Arg0));
			}

			public DataRecordConverter BuildConverter(Type readerType, FieldMap map, Delegate exemplar) {
				var m = exemplar.Method;
				var context = ConverterContext.Create(readerType, m.ReturnType, customConversions);
				var pn = BindAllParameters(context, map, 
					Array.ConvertAll(m.GetParameters(), x => Expression.Parameter(x.ParameterType, x.Name)));
				var arg1 = Expression.Parameter(exemplar.GetType());
				return new DataRecordConverter(Expression.Lambda(Expression.Invoke(arg1, pn), context.Arg0, arg1));
			}

			Expression[] BindAllParameters(ConverterContext context, FieldMap map, IReadOnlyList<ParameterExpression> parameters) {
				var pn = new Expression[parameters.Count];
				for (var i = 0; i != pn.Length; ++i) {
					var root = new ChildBinding();
					if (context.TryReadOrInit(map, parameters[i].Type, parameters[i].Name, ref root, out var reader, throwOnConversionFailure: true) != BindingResult.Ok)
						throw new InvalidOperationException($"Failed to map parameter \"{parameters[i].Name}\"");
					pn[i] = reader.GetReader();
				}
				return pn;
			}
		}

		public DataRecordConverter<TReader, T> GetConverter<TReader, T>(TReader reader) where TReader : IDataReader =>
			converterCache.GetOrAdd(
				reader, ConverterCacheKey.Create(reader, typeof(T)),
				x => recordConverterFactory.BuildConverter(typeof(TReader), x, typeof(T)))
			.ToTyped<TReader, T>();
	
		public Func<TReader, TResult> Compile<TReader, T1, TResult>(TReader reader, Expression<Func<T1, TResult>> selector) where TReader : IDataReader =>
			(Func<TReader, TResult>)GetConverter(reader, selector).Compile();

		public Func<TReader, TResult> Compile<TReader, TArg0, T2, TResult>(TReader reader, Expression<Func<TArg0, T2, TResult>> selector) where TReader : IDataReader =>
			(Func<TReader, TResult>)GetConverter(reader, selector).Compile();

		public Func<TReader, TResult> Compile<TReader, T1, T2, T3, TResult>(TReader reader, Expression<Func<T1, T2, T3, TResult>> selector) where TReader : IDataReader =>
			(Func<TReader, TResult>)GetConverter(reader, selector).Compile();

		public Func<TReader, TResult> Compile<TReader, T1, T2, T3, T4, TResult>(TReader reader, Expression<Func<T1, T2, T3, T4, TResult>> selector) where TReader : IDataReader =>
			(Func<TReader, TResult>)GetConverter(reader, selector).Compile();

		public Func<TReader, TResult> Compile<TReader, T1, T2, T3, T4, T5, TResult>(TReader reader, Expression<Func<T1, T2, T3, T4, T5, TResult>> selector) where TReader : IDataReader =>
			(Func<TReader, TResult>)GetConverter(reader, selector).Compile();

		public Func<TReader, TResult> Compile<TReader, T1, T2, T3, T4, T5, T6, TResult>(TReader reader, Expression<Func<T1, T2, T3, T4, T5, T6, TResult>> selector) where TReader : IDataReader =>
			(Func<TReader, TResult>)GetConverter(reader, selector).Compile();

		public Func<TReader, TResult> Compile<TReader, T1, T2, T3, T4, T5, T6, T7, TResult>(TReader reader, Expression<Func<T1, T2, T3, T4, T5, T6, T7, TResult>> selector) where TReader : IDataReader =>
			(Func<TReader, TResult>)GetConverter(reader, selector).Compile();

		public DataRecordConverter GetConverter<TReader>(TReader reader, LambdaExpression factory) where TReader : IDataReader {
			if (ConverterCacheKey.TryCreate(reader, factory, out var key)) {
				return converterCache.GetOrAdd(
					reader, key,
					x => recordConverterFactory.BuildConverter<TReader>(x, factory));
			}

			return recordConverterFactory.BuildConverter<TReader>(FieldMap.Create(reader), factory);
		}

		public DataRecordConverter GetTrampoline<TReader>(TReader reader, Delegate exemplar) where TReader : IDataReader =>
			converterCache.GetOrAdd(
				reader, ConverterCacheKey.Create(reader, exemplar),
				x => recordConverterFactory.BuildConverter(typeof(TReader), x, exemplar));

		public Delegate CompileTrampoline<TReader>(TReader reader, Delegate exemplar) where TReader : IDataReader =>
			GetTrampoline(reader, exemplar).Compile();

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
				case "System.Data.SqlTypes.SqlByte": return "Byte";
			}
			if(fieldType.IsEnum)
				return Enum.GetUnderlyingType(fieldType).Name;
			return fieldType.Name;
		}
	}
}