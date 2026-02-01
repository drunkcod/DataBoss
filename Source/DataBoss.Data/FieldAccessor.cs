using System;
using System.Collections.Concurrent;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;

namespace DataBoss.Data;

abstract class FieldAccessor<T>
{
		delegate FieldAccessor<T> AccessorFactory(ParameterExpression source, Expression selector, Expression hasValue);

		public abstract bool IsDBNull(T item);
		public abstract object GetValue(T item);
		public abstract TValue GetFieldValue<TValue>(T item);

		public static FieldAccessor<T> Create(ParameterExpression source, Expression selector, Expression hasValue) =>
			Factories.GetOrAdd(selector.Type, FactoryFactory)(source, selector, hasValue);

		static FieldAccessor<T> DoCreate<TField>(ParameterExpression source, Expression selector, Expression hasValue) {
			var get = CompileSelector<TField>(source, selector);
			return hasValue switch {
				null when typeof(TField) == typeof(string) => new StringAccessor<T>((Func<T, string>)(object)get),
				null => new ValueAccessor<T, TField>(get),
				_ => new NullableAccessor<T, TField>(get, CompileSelector<bool>(source, hasValue)),
			};
		}

		static readonly ConcurrentDictionary<Type, AccessorFactory> Factories = new();

		static readonly Func<Type, AccessorFactory> FactoryFactory = type =>
			Lambdas.CreateDelegate<AccessorFactory>(MakeAccessorMethod.MakeGenericMethod(type));

		static readonly MethodInfo MakeAccessorMethod = new AccessorFactory(DoCreate<int>).Method.GetGenericMethodDefinition();

		static Func<T, TResult> CompileSelector<TResult>(ParameterExpression source, Expression selector) =>
			Expression.Lambda<Func<T, TResult>>(selector, source).Compile();
	}

	abstract class FieldAccessor<T, TField>(Func<T, TField> get) : FieldAccessor<T>
	{
		readonly Func<T, TField> get = get;

		public override TValue GetFieldValue<TValue>(T item) {
			var value = get(item);
			if (typeof(TValue) == typeof(TField))
				return Unsafe.As<TField, TValue>(ref value);
			return (TValue)(object)value;
		}

		protected TField GetFieldValue(T item) => get(item);
	}

	sealed class ValueAccessor<T, TField>(Func<T, TField> get) : FieldAccessor<T, TField>(get)
	{
		public override object GetValue(T item) => GetFieldValue(item);
		public override bool IsDBNull(T item) => false;
	}

	sealed class NullableAccessor<T, TField>(Func<T, TField> get, Func<T, bool> hasValue) : FieldAccessor<T, TField>(get)
	{
		readonly Func<T, bool> hasValue = hasValue ?? throw new ArgumentNullException(nameof(hasValue));

		public override object GetValue(T item) => hasValue(item) ? GetFieldValue(item)! : DBNull.Value;
		public override bool IsDBNull(T item) => !hasValue(item);
	}

	sealed class StringAccessor<T>(Func<T, string> get) : FieldAccessor<T, string>(get)
	{
		public override object GetValue(T item) => (object)GetFieldValue(item) ?? DBNull.Value;
		public override bool IsDBNull(T item) => GetFieldValue(item) is null;
	}
