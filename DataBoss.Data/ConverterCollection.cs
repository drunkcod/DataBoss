using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq.Expressions;

namespace DataBoss.Data
{
	public class ConverterCollection
	{
		struct ConverterCollectionItem
		{
			public readonly Type From;
			public readonly Type To;
			public readonly Func<Expression, Expression> MakeConverter;

			public ConverterCollectionItem(Type from, Type to, Func<Expression, Expression> makeConverter) {
				this.From = from;
				this.To = to;
				this.MakeConverter = makeConverter;
			}
		}

		readonly ConverterCollection inner;
		ConverterCollectionItem[] converters;
		int count = 0;

		public ConverterCollection() {
			this.converters = new ConverterCollectionItem[8];
		}

		public ConverterCollection(ConverterCollection other) {
			this.inner = other;
		}

		public void Add<TFrom, TTo>(Func<TFrom, TTo> converter) => Add(typeof(TFrom), converter);

		void Add(Type from, Delegate converter) {
			if(count == converters.Length)
				Array.Resize(ref converters, Math.Max(count << 1, 8));
			converters[count++] = new ConverterCollectionItem(from, converter.Method.ReturnType, x => Expression.Invoke(Expression.Constant(converter), x));
		}

		public bool TryGetConverter(Expression from, Type to, out Expression converter) {
			var found = Array.FindIndex(converters, 0, count, x => x.From == from.Type && x.To == to);
			if(found != -1) {
				converter = converters[found].MakeConverter(from);
				return true;
			}
			if(inner != null)
				return inner.TryGetConverter(from, to, out converter);
			converter = null;
			return false;
		}
	}
}