using System;
using System.Linq.Expressions;
using Cone;

namespace DataBoss.Data
{
	[Describe(typeof(ConverterCollection))]
	public class ConverterCollectionSpec
	{
		[Row(typeof(short), (short)11, typeof(int))]
		[Row(typeof(short), (short)12, typeof(long))]
		[Row(typeof(short), (short)13, typeof(float))]
		[Row(typeof(short), (short)14, typeof(double))]
		[Row(typeof(short), (short)15, typeof(decimal))]
		[Row(typeof(int), 22, typeof(long))]
		[Row(typeof(int), 23, typeof(float))]
		[Row(typeof(int), 24, typeof(double))]
		[Row(typeof(int), 25, typeof(decimal))]
		[Row(typeof(long), 33, typeof(float))]
		[Row(typeof(long), 34, typeof(double))]
		[Row(typeof(long), 35, typeof(decimal))]
		[Row(typeof(byte), (byte)41, typeof(short))]
		[Row(typeof(byte), (byte)42, typeof(ushort))]
		[Row(typeof(byte), (byte)43, typeof(int))]
		[Row(typeof(byte), (byte)44, typeof(uint))]
		[Row(typeof(byte), (byte)45, typeof(long))]
		[Row(typeof(byte), (byte)46, typeof(ulong))]
		[Row(typeof(byte), (byte)47, typeof(float))]
		[Row(typeof(byte), (byte)48, typeof(double))]
		[Row(typeof(byte), (byte)49, typeof(decimal))]
		[Row(typeof(float), (float)3.14, typeof(double))]
		public void standard_conversions(Type from, object input, Type to) {
			var value = Expression.Parameter(from);
			var output = Convert.ChangeType(input, to);
			Expression converter = null;
			Assume.That(() => ConverterCollection.StandardConversions.TryGetConverter(value, to, out converter));

			Check.That(() => Expression.Lambda(converter, value).Compile().DynamicInvoke(input).Equals(output));
		}

		public void supports_multi_level_conversion() {
			var converters = new ConverterCollection(ConverterCollection.StandardConversions);
			converters.Add(new Func<int, string>(x => x.ToString()));
			var input = Expression.Parameter(typeof(byte));
			Expression converter = null;
			Assume.That(() => converters.TryGetConverter(input, typeof(string), out converter));

			Check.That(() => Expression.Lambda(converter, input).Compile().DynamicInvoke((byte)7).Equals("7"));
		}
	}
}
