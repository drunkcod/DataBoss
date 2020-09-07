using System;
using System.Linq.Expressions;
using CheckThat;
using Xunit;

namespace DataBoss.Data
{
	public class ConverterCollectionSpec
	{
		[Theory]
		[InlineData(typeof(short), (short)11, typeof(int))]
		[InlineData(typeof(short), (short)12, typeof(long))]
		[InlineData(typeof(short), (short)13, typeof(float))]
		[InlineData(typeof(short), (short)14, typeof(double))]
		[InlineData(typeof(short), (short)15, typeof(decimal))]
		[InlineData(typeof(int), 22, typeof(long))]
		[InlineData(typeof(int), 23, typeof(float))]
		[InlineData(typeof(int), 24, typeof(double))]
		[InlineData(typeof(int), 25, typeof(decimal))]
		[InlineData(typeof(long), 33, typeof(float))]
		[InlineData(typeof(long), 34, typeof(double))]
		[InlineData(typeof(long), 35, typeof(decimal))]
		[InlineData(typeof(byte), (byte)41, typeof(short))]
		[InlineData(typeof(byte), (byte)42, typeof(ushort))]
		[InlineData(typeof(byte), (byte)43, typeof(int))]
		[InlineData(typeof(byte), (byte)44, typeof(uint))]
		[InlineData(typeof(byte), (byte)45, typeof(long))]
		[InlineData(typeof(byte), (byte)46, typeof(ulong))]
		[InlineData(typeof(byte), (byte)47, typeof(float))]
		[InlineData(typeof(byte), (byte)48, typeof(double))]
		[InlineData(typeof(byte), (byte)49, typeof(decimal))]
		[InlineData(typeof(float), (float)3.14, typeof(double))]
		public void standard_conversions(Type from, object input, Type to) {
			var value = Expression.Parameter(from);
			var output = Convert.ChangeType(input, to);
			Expression converter = null;
			Check.That(() => ConverterCollection.StandardConversions.TryGetConverter(value, to, out converter));

			Check.That(() => Expression.Lambda(converter, value).Compile().DynamicInvoke(input).Equals(output));
		}

		[Fact]
		public void shims_in_standard_conversions_when_possible() {
			var converters = new ConverterCollection();
			converters.Add(new Func<int, string>(x => x.ToString()));
			var input = Expression.Parameter(typeof(byte));
			Expression converter = null;
			Check.That(() => converters.TryGetConverter(input, typeof(string), out converter));

			Check.That(() => Expression.Lambda(converter, input).Compile().DynamicInvoke((byte)7).Equals("7"));
		}

		[Fact]
		public void cant_modify_standard_converter_collections() => Check.Exception<NotSupportedException>(
			() => ConverterCollection.StandardConversions.Add((int x) => x.ToString()));
	}
}
