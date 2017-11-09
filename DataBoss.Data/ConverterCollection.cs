using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq.Expressions;

namespace DataBoss.Data
{
	public struct ConverterCollectionItem
	{
		public readonly Type From;
		public readonly Type To;
		public readonly Func<Expression, Type, Expression> MakeConverter;

		public ConverterCollectionItem(Type from, Type to, Func<Expression, Type, Expression> makeConverter) {
			this.From = from;
			this.To = to;
			this.MakeConverter = makeConverter;
		}
	}

	public class ConverterCollection : IEnumerable<ConverterCollectionItem>
	{
		public static ConverterCollection StandardConversions {
			get {
				Func<Expression, Type, Expression> convert = (x, to) => Expression.Convert(x, to);
				return new ConverterCollection(
					new ConverterCollectionItem(typeof(byte), typeof(short), convert),
					new ConverterCollectionItem(typeof(byte), typeof(ushort), convert),
					new ConverterCollectionItem(typeof(byte), typeof(int), convert),
					new ConverterCollectionItem(typeof(byte), typeof(uint), convert),
					new ConverterCollectionItem(typeof(byte), typeof(long), convert),
					new ConverterCollectionItem(typeof(byte), typeof(ulong), convert),
					new ConverterCollectionItem(typeof(byte), typeof(float), convert),
					new ConverterCollectionItem(typeof(byte), typeof(double), convert),
					new ConverterCollectionItem(typeof(byte), typeof(decimal), convert),
					new ConverterCollectionItem(typeof(short), typeof(int), convert),
					new ConverterCollectionItem(typeof(short), typeof(long), convert),
					new ConverterCollectionItem(typeof(short), typeof(float), convert),
					new ConverterCollectionItem(typeof(short), typeof(double), convert),
					new ConverterCollectionItem(typeof(short), typeof(decimal), convert),
					new ConverterCollectionItem(typeof(int), typeof(long), convert),
					new ConverterCollectionItem(typeof(int), typeof(float), convert),
					new ConverterCollectionItem(typeof(int), typeof(double), convert),
					new ConverterCollectionItem(typeof(int), typeof(decimal), convert),
					new ConverterCollectionItem(typeof(long), typeof(float), convert),
					new ConverterCollectionItem(typeof(long), typeof(double), convert),
					new ConverterCollectionItem(typeof(long), typeof(decimal), convert),
					new ConverterCollectionItem(typeof(float), typeof(double), convert));
			}
		}

		readonly ConverterCollection inner;
		ConverterCollectionItem[] converters;
		int count = 0;

		public ConverterCollection(params ConverterCollectionItem[] items) {
			this.converters = new ConverterCollectionItem[items.Length];
			Array.Copy(items, converters, converters.Length);
			count = items.Length;
		}

		public ConverterCollection(ConverterCollection other) : this() {
			this.inner = other;
		}

		public void Add<TFrom, TTo>(Func<TFrom, TTo> converter) => 
			Add(new ConverterCollectionItem(typeof(TFrom), converter.Method.ReturnType, (x, _) => Expression.Invoke(Expression.Constant(converter), x)));

		public void Add(ConverterCollectionItem item) {
			if(count == converters.Length)
				Array.Resize(ref converters, Math.Max(count + 4, 8));
			converters[count++] = item;
		}

		public bool TryGetConverter(Expression from, Type to, out Expression converter) {
			var found = Array.FindIndex(converters, 0, count, x => x.From == from.Type && x.To == to);
			if(found != -1) {
				converter = converters[found].MakeConverter(from, to);
				return true;
			}
			if(inner != null)
				return inner.TryGetConverter(from, to, out converter);
			converter = null;
			return false;
		}

		public IEnumerator<ConverterCollectionItem> GetEnumerator() =>
			((IEnumerable<ConverterCollectionItem>)converters).GetEnumerator();

		IEnumerator IEnumerable.GetEnumerator() =>
			converters.GetEnumerator();
	}
}