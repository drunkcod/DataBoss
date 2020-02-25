using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Data;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using DataBoss.Data.SqlServer;

namespace DataBoss.Data
{
	public delegate void FillExisting<TReader, T>(TReader reader, ref T target);

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

			public BindingResult BindItem(FieldMap map, in ItemInfo item, out MemberReader reader) {
				if (item.Type.TryGetNullableTargetType(out var baseType)) {
					var r = BindItem(map, new ItemInfo(item.Name, baseType), out var childReader);
					if (r == BindingResult.Ok) {
						
						reader = new MemberReader(
							childReader.Ordinal,
							item.Name,
							Expression.Condition(
								childReader.IsDbNull ?? Expression.Constant(false),
								Expression.Default(item.Type),
								Expression.Convert(childReader.Read, item.Type)), 
							null);
						return BindingResult.Ok;
					}
					reader = default;
					return r;
				}

				if (map.TryGetField(item.Name, out var field)) {
					var o = Expression.Constant(field.Ordinal);
					Expression convertedField;
					var canReadAsItem =
						TryReadFieldAs(field.FieldType, o, item.Type, out convertedField)
						|| (field.ProviderSpecificFieldType != null && TryReadFieldAs(field.ProviderSpecificFieldType, o, item.Type, out convertedField));
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

				if (map.TryGetSubMap(item.Name, out var subMap)) {
					reader = FieldInit(subMap, item);
					return BindingResult.Ok;
				}
				reader = default;
				return BindingResult.NotFound;
			}

			internal InvalidConversionException InvalidConversion(FieldMap map, in ItemInfo item) {
				map.TryGetField(item.Name, out var field);
				return new InvalidConversionException($"Can't read '{item.Name}' of type {item.Type.Name} given {field.FieldType.Name}", ResultType);
			}

			public MemberReader FieldInit(FieldMap map, in ItemInfo item) =>
				GetCtor(map, item)
				?? GetFactoryFunction(map, item)
				?? InitValueType(map, item)
				?? throw new InvalidConversionException("No suitable way found to init " + item.Type, ResultType);

			MemberReader? GetCtor(FieldMap map, in ItemInfo item) {
				var ctors = item.Type.GetConstructors()
					.Select(ctor => (ctor, p: Array.ConvertAll(ctor.GetParameters(), x => new ItemInfo(x.Name, x.ParameterType))))
					.OrderByDescending(x => x.p.Length);
				
				var itemType = item.Type;
				return MakeReader(map, item.Name, ctors, (ctor, ps) => 
					Expression.MemberInit(
						Expression.New(ctor, ps), 
						GetMembers(map, itemType, new HashSet<string>(ctor.GetParameters().Select(x => x.Name), StringComparer.InvariantCultureIgnoreCase))));
			}

			MemberReader? GetFactoryFunction(FieldMap map, in ItemInfo item) {
				var factoryFuns = item.Type
					.GetMethods(BindingFlags.Static | BindingFlags.Public)
					.Where(x => x.GetCustomAttribute(typeof(ConsiderAsCtorAttribute)) != null)
					.Select(f => (fun: f, p: Array.ConvertAll(f.GetParameters(), x => new ItemInfo(x.Name, x.ParameterType))))
					.OrderByDescending(x => x.p.Length);
				
				return MakeReader(map, item.Name, factoryFuns, Expression.Call);
			}

			MemberReader? MakeReader<T>(FieldMap map, string itemName, IEnumerable<(T, ItemInfo[])> xs, Func<T, Expression[], Expression> makeExpr) {
				foreach (var (ctor, p) in xs) {
					var pn = new MemberReader[p.Length];
					if (TryMapParameters(map, p, pn)) {
						var nullability = pn
							.Where(x => x.Read.Type.IsValueType && x.IsDbNull != null)
							.Select(x => (x.Name, x.IsDbNull))
							.ToArray();

						return new MemberReader(
							map.MinOrdinal,
							itemName,
							makeExpr(ctor, Array.ConvertAll(pn, x => x.IsDbNull == null  
								? x.Read
								: Expression.Condition(x.IsDbNull, Expression.Default(x.Read.Type), x.Read))),
							nullability);
					}
				}

				return null;
			}

			MemberReader? InitValueType(FieldMap map, in ItemInfo item) {
				if(item.Type.IsValueType)
					return new MemberReader(map.MinOrdinal, item.Name, Expression.MemberInit(Expression.New(item.Type), GetMembers(map, item.Type)), null);
				return null;
			}

			public ArraySegment<MemberAssignment> GetMembers(FieldMap map, Type targetType, HashSet<string> excludedMembers = null) {
				var fields = targetType.GetFields().Where(x => !x.IsInitOnly).Select(x => (Item: new ItemInfo(x.Name, x.FieldType), Member: (MemberInfo)x));
				var props = targetType.GetProperties().Where(x => x.CanWrite).Select(x => (Item: new ItemInfo(x.Name, x.PropertyType), Member: (MemberInfo)x));
				var allMembers = fields.Concat(props);
				var members = (excludedMembers == null ? allMembers : allMembers.Where(x => !excludedMembers.Contains(x.Item.Name))).ToArray();
				var ordinals = new int[members.Length];
				var bindings = new MemberAssignment[members.Length];
				var found = 0;
				foreach (var x in members) {
					switch(BindItem(map, x.Item, out var reader)) {
						case BindingResult.InvalidCast: throw InvalidConversion(map, x.Item);
						case BindingResult.NotFound:
							if (x.Member.GetCustomAttribute(typeof(RequiredAttribute), false) != null)
								throw new ArgumentException("Failed to set required member.", x.Item.Name);
							else continue;

						case BindingResult.Ok:
							ordinals[found] = reader.Ordinal;
							bindings[found] = Expression.Bind(x.Member, ReadOrDefault(reader));
							++found;
							break;
					}
				}
				Array.Sort(ordinals, bindings, 0, found);
				return new ArraySegment<MemberAssignment>(bindings, 0, found);
			}

			bool TryMapParameters(FieldMap map, ItemInfo[] parameters, MemberReader[] exprs) {
				for (var i = 0; i != parameters.Length; ++i) {
					if (BindItem(map, parameters[i], out exprs[i]) != BindingResult.Ok)
						return false;
				}
				return true;
			}
		}

		readonly struct ItemInfo 
		{
			public ItemInfo(string name, Type type) {
				this.Name = name;
				this.Type = type;
			}

			public readonly string Name;
			public readonly Type Type;
		}

		readonly struct MemberReader
		{
			public MemberReader(int ordinal, string name, Expression reader, IReadOnlyCollection<(string Name, Expression IsDbNull)> isDbNull) {
				this.Ordinal = ordinal;
				this.Name = name;
				this.Read = reader;
				if(isDbNull != null && isDbNull.Any(x => x.IsDbNull == null))
					throw new InvalidOperationException("Null nullability check.");
				this.NullableFields = isDbNull;
			}

			public readonly int Ordinal;
			public readonly string Name;
			public readonly Expression Read;
			public Expression IsDbNull {
				get {
					if(NullableFields == null || NullableFields.Count == 0)
						return null;
					return AnyOf(NullableFields.Select(x => x.IsDbNull));
				}
			}

			public readonly IReadOnlyCollection<(string Name, Expression IsDbNull)> NullableFields;
		}

		static Expression ReadOrDefault(in MemberReader reader) =>
			reader.IsDbNull == null ? reader.Read : Expression.Condition(reader.IsDbNull, MakeDefault(reader.Read.Type), reader.Read);

		static Expression GuardedRead(in MemberReader reader) {
			if(reader.IsDbNull == null)
				return reader.Read;
			return GuardedExpression(reader.Read, reader.IsDbNull, reader.NullableFields);
		}

		static Expression GuardedInvoke(Expression body, MemberReader[] args) {
			var isNull = AnyOf(args.Where(x => x.Read.Type.IsPrimitive).Select(x => x.IsDbNull));
			body = Expression.Invoke(body, Array.ConvertAll(args, x => x.Read));
			if (isNull == null)
				return body;

			return GuardedExpression(body, isNull, args
				.Where(x => x.Read.Type.IsPrimitive && x.IsDbNull != null)
				.Select(x => (x.Name, x.IsDbNull)));
		}

		static Expression GuardedExpression(Expression expr, Expression isNull, IEnumerable<(string Name, Expression IsDbNull)> fields) {
			var nullFields = Expression.NewArrayInit(
				typeof(string),
				fields.Select(x =>
					Expression.Condition(x.IsDbNull,
						Expression.Constant(x.Name),
						Expression.Constant(null, typeof(string)))));

			var @throw = Expression.Throw(
				Expression.New(
					typeof(DataRowNullCastException).GetConstructor(new[] { typeof(string[]) }), 
					nullFields),
				expr.Type);

			return Expression.Condition(isNull, @throw, expr);
		}

		class DataRowNullCastException : InvalidCastException
		{
			public DataRowNullCastException(string[] nullFields) :
				base(string.Format("'{0}' was null.", string.Join(", ", nullFields.Where(x => !string.IsNullOrEmpty(x))))) { }
		}

		static Expression MakeDefault(Type type) =>
			Expression.Default(type);

		enum BindingResult
		{
			Ok,
			NotFound,
			InvalidCast
		}

		class DataRecordConverterFactory
		{
			readonly ConverterCollection customConversions;

			public DataRecordConverterFactory(ConverterCollection customConversions) { this.customConversions = customConversions; }

			public DataRecordConverter BuildConverter(Type readerType, FieldMap map, Type result) {
				var context = ConverterContext.Create(readerType, result, customConversions);
				var reader = context.FieldInit(map, new ItemInfo(null, context.ResultType));
				return new DataRecordConverter(Expression.Lambda(
					GuardedRead(reader), context.Arg0));
			}

			public DataRecordConverter BuildConverter<TReader>(FieldMap map, LambdaExpression resultFactory) where TReader : IDataReader {
				var context = ConverterContext.Create(typeof(TReader), resultFactory.Type, customConversions);
				return new DataRecordConverter(Expression.Lambda(
					GuardedInvoke(
						resultFactory, 
						BindAllParameters(context, map, resultFactory.Parameters.Select(x => new ItemInfo(x.Name, x.Type)).ToArray())),
					context.Arg0));
			}

			public DataRecordConverter BuildConverter(Type readerType, FieldMap map, Delegate exemplar) {
				var m = exemplar.Method;
				var context = ConverterContext.Create(readerType, m.ReturnType, customConversions);
				var pn = BindAllParameters(context, map,
					Array.ConvertAll(m.GetParameters(), x => new ItemInfo(x.Name, x.ParameterType)));
				var arg1 = Expression.Parameter(exemplar.GetType());
				return new DataRecordConverter(Expression.Lambda(GuardedInvoke(arg1, pn), context.Arg0, arg1));
			}

			MemberReader[] BindAllParameters(ConverterContext context, FieldMap map, ItemInfo[] parameters) {
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

		public ConverterFactory(ConverterCollection customConversions) : this(customConversions, new ConcurrentConverterCache())
		{ }

		public ConverterFactory(ConverterCollection customConversions, IConverterCache converterCache) {
			this.recordConverterFactory = new DataRecordConverterFactory(new ConverterCollection(customConversions));
			this.converterCache = converterCache;
		}

		public static ConverterFactory Default = new ConverterFactory(null, new ConcurrentConverterCache());

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
		
		public FillExisting<TReader, T> GetReadInto<TReader, T>(TReader reader) where TReader : IDataReader =>
			(FillExisting<TReader, T>)readIntoCache.GetOrAdd(ConverterCacheKey.Into(reader, typeof(TReader), typeof(T)), delegate
			{
				var fields = FieldMap.Create(reader);
				var context = ConverterContext.Create(typeof(TReader), typeof(T), null);
				var members = context.GetMembers(fields, context.ResultType);

				var target = Expression.Parameter(typeof(T).MakeByRefType(), "target");

				return Expression.Lambda<FillExisting<TReader, T>>(
					Expression.Block(
						members.Select(x => Expression.Assign(Expression.MakeMemberAccess(target, x.Member), x.Expression))),
					context.Arg0,
					target).Compile();
			});

		public LambdaExpression GetReadInto(IDataReader reader, Type targetType) {
			var fields = FieldMap.Create(reader);
			var context = ConverterContext.Create(typeof(IDataReader), targetType, null);
			var members = context.GetMembers(fields, context.ResultType);

			var target = Expression.Parameter(targetType.MakeByRefType(), "target");

			return Expression.Lambda(
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

		static Expression AnyOf(IEnumerable<Expression> exprs) =>
			exprs.Aggregate((Expression)null, OrElse);

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