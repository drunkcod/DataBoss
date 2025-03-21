using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Data;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace DataBoss.Data
{
	public delegate void Updater<TSource, T>(TSource reader, ref T target);

	public readonly struct ConverterItemInfo
	{
		public ConverterItemInfo(string name, Type type) {
			this.Name = name;
			this.Type = type;
		}

		public readonly string Name;
		public readonly Type Type;
	}

	static class Util
	{
		public static Expression AnyOf(IEnumerable<Expression> exprs) =>
			exprs.Aggregate((Expression)null, OrElse);

		static Expression OrElse(Expression left, Expression right) {
			if (left == null)
				return right;
			if (right == null)
				return left;
			return Expression.OrElse(left, right);
		}

		public static string MapFieldType(Type fieldType) {
			switch (fieldType.FullName) {
				case "System.Single": return "Float";
				case "System.Byte[]":
				case "System.TimeSpan":
				case "System.Object": return "Value";
				case "System.Data.SqlTypes.SqlByte": return "Byte";
			}
			if (fieldType.IsEnum)
				return Enum.GetUnderlyingType(fieldType).Name;
			return fieldType.Name;
		}

	}

	public readonly struct MemberReader
	{
		readonly Expression isDbNull;

		public MemberReader(int ordinal, string name, Expression reader, IReadOnlyCollection<(string Name, Expression IsDbNull)> isDbNull) {
			if (isDbNull != null && isDbNull.Any(x => x.IsDbNull == null))
				throw new InvalidOperationException("Null nullability check.");

			this.Ordinal = ordinal;
			this.Name = name;
			this.ReadRaw = reader;
			this.NullableFields = isDbNull;
			this.isDbNull = (NullableFields == null || NullableFields.Count == 0) ? null : Util.AnyOf(NullableFields.Select(x => x.IsDbNull));
		}

		public readonly int Ordinal;
		public readonly string Name;
		public readonly Expression ReadRaw;
		public Expression IsDbNull {
			get { return isDbNull; }
		}

		public Expression Read =>
			IsDbNull is null ? ReadRaw : Expression.Condition(IsDbNull, Expression.Default(ReadRaw.Type), ReadRaw);


		public readonly IReadOnlyCollection<(string Name, Expression IsDbNull)> NullableFields;
	}

	public enum BindingResult
	{
		Ok,
		NotFound,
		InvalidCast
	}

	public class ConverterContext
	{
		readonly ConverterCollection converters;
		readonly MethodInfo getFieldValueT;

		ConverterContext(ParameterExpression arg0, MethodInfo isDBNull, Type resultType, ConverterCollection converters) {
			this.Arg0 = arg0;
			this.IsDBNull = isDBNull;
			this.ResultType = resultType;
			this.converters = converters;
			this.getFieldValueT = arg0.Type.GetMethod("GetFieldValue");
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
			if (TryReadField(fieldType, ordinal, out var readRaw)
			&& TryConvertField(readRaw, itemType, out reader))
				return true;

			reader = null;
			return false;
		}

		public bool TryReadField(Type fieldType, Expression ordinal, out Expression reader) {
			if (TryGetGetMethod(fieldType, out var getter)) {
				reader = Expression.Call(Arg0, getter, ordinal);
				return true;
			}

			reader = null;
			return false;
		}

		bool TryGetGetMethod(Type fieldType, out MethodInfo getter) {
			getter = GetGetMethod(Arg0.Type, "Get" + fieldType.Name, fieldType);
			if (getter == null && getFieldValueT != null)
				getter = getFieldValueT.MakeGenericMethod(fieldType);
			if (getter == null)
				getter = GetGetMethod(Arg0.Type, "Get" + Util.MapFieldType(fieldType), fieldType);

			return getter != null;
		}

		static MethodInfo GetGetMethod(Type arg0, string name, Type type) {
			var found = arg0.GetMethod(name) ?? typeof(IDataRecord).GetMethod(name);

			if (found != null && ParametersEqual(found, typeof(int)))
				return found;
			return null;
		}

		static bool ParametersEqual(MethodInfo method, params Type[] parameterTypes) {
			var ps = method.GetParameters();

			if (ps.Length != parameterTypes.Length)
				return false;

			for (var i = 0; i != ps.Length; ++i)
				if (ps[i].ParameterType != parameterTypes[i])
					return false;

			return true;
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

			if (CanConvert(rawField, to)) {
				converter = Expression.Convert(rawField, to);
				return true;
			}

			return false;
		}

		static bool CanConvert(Expression rawField, Type to) =>
			IsByteArray(rawField, to)
			|| IsEnum(rawField, to)
			|| IsTimeSpan(rawField, to)
			|| ToIsAssignableFrom(rawField, to)
			|| FromIsCastableTo(rawField, to);

		static bool IsByteArray(Expression rawField, Type to) =>
			rawField.Type == typeof(object) && to == typeof(byte[]);

		static bool IsTimeSpan(Expression rawField, Type to) =>
			rawField.Type == typeof(object) && to == typeof(TimeSpan);

		static bool IsEnum(Expression rawField, Type to) =>
			to.IsEnum && Enum.GetUnderlyingType(to) == rawField.Type;

		static bool ToIsAssignableFrom(Expression rawField, Type to) {
			var t = new[] { rawField.Type };
			var cast = to.GetMethod("op_Implicit", t) ?? to.GetMethod("op_Explicit", t);

			return cast != null && to.IsAssignableFrom(cast.ReturnType);
		}

		static bool FromIsCastableTo(Expression rawField, Type to) =>
			rawField.Type.GetMethods(BindingFlags.Public | BindingFlags.Static)
			.Any(x => x.IsSpecialName && (x.Name == "op_Implicit" || x.Name == "op_Explicit") && x.ReturnType == to);

		public Expression IsNull(Expression o) => Expression.Call(Arg0, IsDBNull, o);

		public Expression DbNullToDefault(FieldMapItem field, Expression o, Type itemType, Expression readIt) {
			if (!field.CanBeNull)
				return readIt;
			return Expression.Condition(
				Expression.Call(Arg0, IsDBNull, o),
				Expression.Default(itemType),
				readIt);
		}

		public BindingResult BindItem(FieldMap map, in ConverterItemInfo item, out MemberReader reader) {
			if (item.Type.TryGetNullableTargetType(out var baseType))
				return BindNullable(map, item, baseType, out reader);

			if (map.TryGetField(item.Name, out var field))
				return BindField(item, field, out reader);

			if (map.TryGetSubMap(item.Name, out var subMap)) {
				reader = FieldInit(subMap, item);
				return BindingResult.Ok;
			}
			reader = default;
			return BindingResult.NotFound;
		}

		private BindingResult BindField(ConverterItemInfo item, FieldMapItem field, out MemberReader reader) {
			var o = Expression.Constant(field.Ordinal);
			var canReadAsItem =
				TryReadFieldAs(field.FieldType, o, item.Type, out var convertedField)
				|| TryReadProviderSpecificFieldAs(item, field, o, ref convertedField);

			if (!canReadAsItem) {
				reader = default;
				return BindingResult.InvalidCast;
			}

			var thisNull = field.CanBeNull
			? new[] { (item.Name, IsNull(o)) }
			: null;
			reader = new MemberReader(field.Ordinal, item.Name, convertedField, thisNull);
			return BindingResult.Ok;
		}

		private BindingResult BindNullable(FieldMap map, ConverterItemInfo item, Type baseType, out MemberReader reader) {
			var r = BindItem(map, new ConverterItemInfo(item.Name, baseType), out var childReader);
			reader = r != BindingResult.Ok ? default : new MemberReader(
				childReader.Ordinal,
				item.Name,
				Expression.Condition(
					childReader.IsDbNull ?? Expression.Constant(false),
					Expression.Default(item.Type),
					Expression.Convert(childReader.ReadRaw, item.Type)),
				null);
			return r;
		}

		private bool TryReadProviderSpecificFieldAs(ConverterItemInfo item, FieldMapItem field, ConstantExpression o, ref Expression convertedField) {
			if (field.ProviderSpecificFieldType == null)
				return false;

			if (TryReadFieldAs(field.ProviderSpecificFieldType, o, item.Type, out convertedField))
				return true;

			var getProviderSpecificValue = Arg0.Type.GetMethod("GetProviderSpecificValue");
			if (getProviderSpecificValue == null)
				return false;

			var readProviderSpecific = Expression.Convert(
				Expression.Call(Arg0, getProviderSpecificValue, o),
				field.ProviderSpecificFieldType);

			return TryConvertField(readProviderSpecific, item.Type, out convertedField);
		}

		internal InvalidConversionException InvalidConversion(FieldMap map, in ConverterItemInfo item) {
			map.TryGetField(item.Name, out var field);
			return new InvalidConversionException($"Can't read '{item.Name}' of type {item.Type.Name} given {field.FieldType.Name}", ResultType);
		}

		public MemberReader FieldInit(FieldMap map, in ConverterItemInfo item) =>
			GetCtor(map, item)
			?? GetFactoryFunction(map, item)
			?? InitValueType(map, item)
			?? ReadScalar(map, item)
			?? throw new InvalidConversionException($"No suitable way found to init {item.Name ?? "$"} of type {item.Type}", ResultType);

		MemberReader? GetCtor(FieldMap map, in ConverterItemInfo item) {
			if (map.Count == 1 && item.Type == map.Single().Item.FieldType)
				return null;

			var ctors = item.Type.GetConstructors()
				.Select(ctor => (ctor, p: Array.ConvertAll(ctor.GetParameters(), x => new ConverterItemInfo(x.Name, x.ParameterType))))
				.OrderByDescending(x => x.p.Length);

			var itemType = item.Type;
			return MakeReader(map, item.Name, ctors, (ctor, ps) =>
				Expression.MemberInit(
					Expression.New(ctor, ps),
					GetMembers(map, itemType, new HashSet<string>(ctor.GetParameters().Select(x => x.Name), StringComparer.InvariantCultureIgnoreCase))));
		}

		MemberReader? GetFactoryFunction(FieldMap map, in ConverterItemInfo item) {
			var factoryFuns = item.Type
				.GetMethods(BindingFlags.Static | BindingFlags.Public)
				.Where(x => x.GetCustomAttribute(typeof(ConsiderAsCtorAttribute)) != null)
				.Select(f => (fun: f, p: Array.ConvertAll(f.GetParameters(), x => new ConverterItemInfo(x.Name, x.ParameterType))))
				.OrderByDescending(x => x.p.Length);

			return MakeReader(map, item.Name, factoryFuns, Expression.Call);
		}

		MemberReader? MakeReader<T>(FieldMap map, string itemName, IEnumerable<(T, ConverterItemInfo[])> xs, Func<T, Expression[], Expression> makeExpr) {
			foreach (var (ctor, p) in xs) {
				var pn = new MemberReader[p.Length];
				if (TryMapParameters(map, p, pn)) {
					var nullability = pn
						.Where(x => x.ReadRaw.Type.IsValueType && x.IsDbNull != null)
						.Select(x => (x.Name, x.IsDbNull))
						.ToArray();

					return new MemberReader(
						map.MinOrdinal,
						itemName,
						makeExpr(ctor, Array.ConvertAll(pn, x => x.Read)),
						nullability);
				}
			}

			return null;
		}

		MemberReader? InitValueType(FieldMap map, in ConverterItemInfo item) {
			if (!item.Type.IsValueType)
				return null;

			var foundMembers = GetMembers(map, item.Type);
			if (foundMembers.Count == 0)
				return null;

			return new MemberReader(map.MinOrdinal, item.Name, Expression.MemberInit(Expression.New(item.Type), foundMembers), null);
		}

		MemberReader? ReadScalar(FieldMap map, in ConverterItemInfo item) {
			var (fieldName, field) = map.First();
			var o = Expression.Constant(field.Ordinal);

			var isNull = IsNull(o);
			if (TryReadFieldAs(field.FieldType, o, item.Type, out var read)) {
				if (item.Type.IsValueType && !item.Type.IsNullable())
					return new MemberReader(field.Ordinal, item.Name, read, new[] { (fieldName, isNull) });
				else return new MemberReader(field.Ordinal, item.Name, Expression.Condition(isNull, Expression.Default(item.Type), read), null);
			}

			return null;
		}

		public ArraySegment<MemberAssignment> GetMembers(FieldMap map, Type targetType, HashSet<string> excludedMembers = null, bool defaultOnNull = true) {
			var fields = targetType.GetFields().Where(x => !x.IsInitOnly).Select(x => (Item: new ConverterItemInfo(GetName(x), x.FieldType), Member: (MemberInfo)x));
			var props = targetType.GetProperties().Where(x => x.CanWrite).Select(x => (Item: new ConverterItemInfo(GetName(x), x.PropertyType), Member: (MemberInfo)x));
			var allMembers = fields.Concat(props);
			var members = (excludedMembers == null ? allMembers : allMembers.Where(x => !excludedMembers.Contains(x.Item.Name))).ToArray();
			var ordinals = new int[members.Length];
			var bindings = new MemberAssignment[members.Length];
			var found = 0;
			foreach (var x in members) {
				switch (BindItem(map, x.Item, out var reader)) {
					case BindingResult.InvalidCast: throw InvalidConversion(map, x.Item);
					case BindingResult.NotFound:
						if (x.Member.GetCustomAttribute(typeof(RequiredAttribute), false) != null)
							throw new ArgumentException("Failed to set required member.", x.Item.Name);
						else continue;

					case BindingResult.Ok:
						ordinals[found] = reader.Ordinal;
						bindings[found] = Expression.Bind(x.Member, defaultOnNull ? reader.Read : reader.ReadRaw);
						++found;
						break;
				}
			}
			Array.Sort(ordinals, bindings, 0, found);
			return new ArraySegment<MemberAssignment>(bindings, 0, found);
		}

		static Expression MakeDefault(Type type) =>
			Expression.Default(type);

		static string GetName(MemberInfo member) {
			var column = member.GetCustomAttribute<System.ComponentModel.DataAnnotations.Schema.ColumnAttribute>();
			return column?.Name ?? member.Name;
		}

		bool TryMapParameters(FieldMap map, ConverterItemInfo[] parameters, MemberReader[] exprs) {
			for (var i = 0; i != parameters.Length; ++i) {
				if (BindItem(map, parameters[i], out exprs[i]) != BindingResult.Ok)
					return false;
			}
			return true;
		}
	}

	public class ConverterFactory
	{
		static Expression GuardedRead(in MemberReader reader) {
			if (reader.IsDbNull == null)
				return reader.ReadRaw;
			return GuardedExpression(reader.ReadRaw, reader.IsDbNull, reader.NullableFields);
		}

		static Expression GuardedInvoke(Expression body, MemberReader[] args) {
			var isNull = Util.AnyOf(args.Where(x => x.ReadRaw.Type.IsPrimitive).Select(x => x.IsDbNull));
			body = Expression.Invoke(body, Array.ConvertAll(args, x => x.Read));
			if (isNull == null)
				return body;

			return GuardedExpression(body, isNull, args
				.Where(x => x.ReadRaw.Type.IsPrimitive && x.IsDbNull != null)
				.Select(x => (x.Name, x.IsDbNull)));
		}

		static Expression GuardedExpression(Expression expr, Expression isNull, IEnumerable<(string Name, Expression IsDbNull)> fields) {
			var nullFields = Expression.NewArrayInit(
				typeof(string),
				fields.Select(x =>
					Expression.Condition(x.IsDbNull,
						Expression.Constant(x.Name, typeof(string)),
						Expression.Constant(null, typeof(string)))));

			var @throw = Expression.Throw(
				Expression.New(
					typeof(DataRowNullCastException).GetConstructor([typeof(string[])]),
					nullFields),
				expr.Type);

			return Expression.Condition(isNull, @throw, expr);
		}

		class DataRowNullCastException : InvalidCastException
		{
			public DataRowNullCastException(string[] nullFields) :
				base(string.Format("'{0}' was null.", string.Join(", ", nullFields.Where(x => !string.IsNullOrEmpty(x))))) { }
		}

		class DataRecordConverterFactory
		{
			readonly ConverterCollection customConversions;

			public DataRecordConverterFactory(ConverterCollection customConversions) { this.customConversions = customConversions ?? new ConverterCollection(); }

			public DataRecordConverter BuildConverter(Type readerType, FieldMap map, Type result) {
				var context = ConverterContext.Create(readerType, result, customConversions);
				var reader = context.FieldInit(map, new ConverterItemInfo(null, context.ResultType));
				return new DataRecordConverter(Expression.Lambda(
					GuardedRead(reader), context.Arg0));
			}

			public DataRecordConverter BuildConverter<TReader>(FieldMap map, LambdaExpression resultFactory) where TReader : IDataReader {
				var context = ConverterContext.Create(typeof(TReader), resultFactory.Type, customConversions);
				return new DataRecordConverter(Expression.Lambda(
					GuardedInvoke(
						resultFactory,
						BindAllParameters(context, map, [.. resultFactory.Parameters.Select(x => new ConverterItemInfo(x.Name, x.Type))])),
					context.Arg0));
			}

			public DataRecordConverter BuildConverter(Type readerType, FieldMap map, Delegate exemplar) {
				var m = exemplar.Method;
				var context = ConverterContext.Create(readerType, m.ReturnType, customConversions);
				var pn = BindAllParameters(context, map,
					Array.ConvertAll(m.GetParameters(), x => new ConverterItemInfo(x.Name, x.ParameterType)));
				var arg1 = Expression.Parameter(exemplar.GetType());
				return new DataRecordConverter(Expression.Lambda(GuardedInvoke(arg1, pn), context.Arg0, arg1));
			}

			MemberReader[] BindAllParameters(ConverterContext context, FieldMap map, ConverterItemInfo[] parameters) {
				var pn = new MemberReader[parameters.Length];
				for (var i = 0; i != pn.Length; ++i) {
					switch (context.BindItem(map, parameters[i], out pn[i])) {
						case BindingResult.InvalidCast: throw context.InvalidConversion(map, parameters[i]);
						case BindingResult.NotFound: throw new InvalidOperationException($"Failed to map parameter \"{parameters[i].Name}\"");
					}
				}
				return pn;
			}
		}

		readonly DataRecordConverterFactory recordConverterFactory;
		readonly IConverterCache converterCache;
		readonly ConcurrentDictionary<ConverterCacheKey, Delegate> readIntoCache = new ConcurrentDictionary<ConverterCacheKey, Delegate>();

		public ConverterFactory(ConverterCollection customConversions) : this(customConversions, new ConcurrentConverterCache()) { }

		public ConverterFactory(ConverterCollection customConversions, IConverterCache converterCache) {
			this.recordConverterFactory = new DataRecordConverterFactory(customConversions);
			this.converterCache = converterCache;
		}

		public static ConverterFactory Default = new(null, new ConcurrentConverterCache());

		public static DataRecordConverter<TReader, T> GetConverter<TReader, T>(TReader reader, ConverterCollection customConversions) where TReader : IDataReader {
			var recordConverterFactory = new DataRecordConverterFactory(customConversions);
			return recordConverterFactory.BuildConverter(typeof(TReader), FieldMap.Create(reader), typeof(T)).ToTyped<TReader, T>();
		}

		public Func<TReader, TResult> Compile<TReader, T1, TResult>(TReader reader, Expression<Func<T1, TResult>> selector) where TReader : IDataReader =>
			(Func<TReader, TResult>)GetConverter(reader, selector).Compile();

		public Func<TReader, TResult> Compile<TReader, T1, T2, TResult>(TReader reader, Expression<Func<T1, T2, TResult>> selector) where TReader : IDataReader =>
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

		public DataRecordConverter<TReader, T> GetConverter<TReader, T>(TReader reader) where TReader : IDataReader =>
			converterCache.GetOrAdd(
				reader, ConverterCacheKey.Create(reader, typeof(TReader), typeof(T)),
				x => recordConverterFactory.BuildConverter(typeof(TReader), x, typeof(T)))
			.ToTyped<TReader, T>();

		public Updater<IDataReader, T> GetReadInto<T>(IDataReader reader) =>
			(Updater<IDataReader, T>)readIntoCache.GetOrAdd(ConverterCacheKey.Into(reader, typeof(IDataReader), typeof(T)), delegate {
				return (Updater<IDataReader, T>)GetReadInto(reader, typeof(IDataReader), typeof(T)).Compile();
			});

		public LambdaExpression GetReadInto(IDataReader reader, Type readerType, Type targetType) {
			var fields = FieldMap.Create(reader);
			var context = ConverterContext.Create(readerType, targetType, null);
			var members = context.GetMembers(fields, context.ResultType, defaultOnNull: false);

			var target = Expression.Parameter(targetType.MakeByRefType(), "target");

			return Expression.Lambda(
				typeof(Updater<,>).MakeGenericType(readerType, targetType),
				Expression.Block(
					members.Select(x => Expression.Assign(Expression.MakeMemberAccess(target, x.Member), x.Expression))),
				context.Arg0,
				target);
		}

		public DataRecordConverter GetConverter<TReader>(TReader reader, LambdaExpression factory) where TReader : IDataReader {
			if (ConverterCacheKey.TryCreate(reader, factory, out var key)) {
				return converterCache.GetOrAdd(
					reader, key,
					x => recordConverterFactory.BuildConverter<TReader>(x, factory));
			}

			return recordConverterFactory.BuildConverter<TReader>(FieldMap.Create(reader), factory);
		}

		public DataRecordConverter<TReader, T> BuildConverter<TReader, T>(FieldMap fields) =>
			recordConverterFactory.BuildConverter(typeof(TReader), fields, typeof(T)).ToTyped<TReader, T>();

		public DataRecordConverter GetTrampoline<TReader>(TReader reader, Delegate exemplar) where TReader : IDataReader =>
			converterCache.GetOrAdd(
				reader, ConverterCacheKey.Create(reader, exemplar),
				x => recordConverterFactory.BuildConverter(typeof(TReader), x, exemplar));

		public Delegate CompileTrampoline<TReader>(TReader reader, Delegate exemplar) where TReader : IDataReader =>
			GetTrampoline(reader, exemplar).Compile();
	}
}