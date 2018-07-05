using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

namespace DataBoss.Data
{
	public struct ConverterCollectionItem
	{
		public bool IsEmpty => From == null;

		public readonly Type From;
		public readonly Type To;
		public readonly Func<Expression, Expression> Convert;

		public ConverterCollectionItem(Type from, Type to, Func<Expression, Expression> makeConverter) {
			this.From = from;
			this.To = to;
			this.Convert = makeConverter;
		}
	}

	public class ConverterCollection : IEnumerable<ConverterCollectionItem>
	{
		static Expression To<T>(Expression input) => Expression.Convert(input, typeof(T));

		public static ConverterCollection StandardConversions => new ConverterCollection(
			new ConverterCollectionItem(typeof(byte), typeof(short), To<short>),
			new ConverterCollectionItem(typeof(byte), typeof(ushort), To<ushort>),
			new ConverterCollectionItem(typeof(byte), typeof(int), To<int>),
			new ConverterCollectionItem(typeof(byte), typeof(uint), To<uint>),
			new ConverterCollectionItem(typeof(byte), typeof(long), To<long>),
			new ConverterCollectionItem(typeof(byte), typeof(ulong), To<ulong>),
			new ConverterCollectionItem(typeof(byte), typeof(float), To<float>),
			new ConverterCollectionItem(typeof(byte), typeof(double), To<double>),
			new ConverterCollectionItem(typeof(byte), typeof(decimal), To<decimal>),
			new ConverterCollectionItem(typeof(short), typeof(int), To<int>),
			new ConverterCollectionItem(typeof(short), typeof(long), To<long>),
			new ConverterCollectionItem(typeof(short), typeof(float), To<float>),
			new ConverterCollectionItem(typeof(short), typeof(double), To<double>),
			new ConverterCollectionItem(typeof(short), typeof(decimal), To<decimal>),
			new ConverterCollectionItem(typeof(int), typeof(long), To<long>),
			new ConverterCollectionItem(typeof(int), typeof(float), To<float>),
			new ConverterCollectionItem(typeof(int), typeof(double), To<double>),
			new ConverterCollectionItem(typeof(int), typeof(decimal), To<decimal>),
			new ConverterCollectionItem(typeof(long), typeof(float), To<float>),
			new ConverterCollectionItem(typeof(long), typeof(double), To<double>),
			new ConverterCollectionItem(typeof(long), typeof(decimal), To<decimal>),
			new ConverterCollectionItem(typeof(float), typeof(double), To<double>));

		readonly ConverterCollection inner;
		ICollection<ConverterCollectionItem> converters;

		public ConverterCollection() {
			this.converters = new List<ConverterCollectionItem>();
		}

		public ConverterCollection(params ConverterCollectionItem[] items) {
			this.converters = items;
		}

		public ConverterCollection(ConverterCollection other) : this() {
			this.inner = other;
		}

		public void Add<TFrom, TTo>(Func<TFrom, TTo> converter) => 
			Add(new ConverterCollectionItem(typeof(TFrom), converter.Method.ReturnType, x => Expression.Invoke(Expression.Constant(converter), x)));

		public void Add(ConverterCollectionItem item) =>
			converters.Add(item);

		public bool TryGetConverter(Expression from, Type to, out Expression converter) {
			var found = converters.FirstOrDefault(x => x.From == from.Type && x.To == to);
			if(!found.IsEmpty) {
				converter = found.Convert(from);
				return true;
			}

			if(inner != null && inner.TryGetConverter(from, to, out converter))
				return true;
			
			foreach(var item in converters.Where(x => x.To == to))
				if(StandardConversions.TryGetConverter(from, item.From, out var implicitCast)) { 
					converter = item.Convert(implicitCast);
					return true;
				}
	
			converter = null;
			return false;
		}

		public IEnumerator<ConverterCollectionItem> GetEnumerator() => 
			converters.GetEnumerator();

		IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
	}
}