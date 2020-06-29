using System;
using System.Data;
using Cone;
using DataBoss.Data;
using Xunit;

namespace DataBoss.Specs.Data
{
	public class ConverterFactorySpec
	{
		[Fact]
		public void ctor_dbnull_reference_arg() {
			var r = SequenceDataReader.Items(new { Value = (string)null });
			var converter = ConverterFactory.Default.GetConverter<IDataReader, ValueRow<string>>(r);
			r.Read();
			Check.That(
				() => converter.Compiled(r) != null,
				() => converter.Compiled(r).Value == null);
		}

		[Fact]
		public void ctor_dbnull_nullable_arg() {
			var r = SequenceDataReader.Items(new { Value = (int?)null });
			var converter = ConverterFactory.Default.GetConverter<IDataReader, ValueRow<int?>>(r);
			r.Read();
			Check.That(
				() => converter.Compiled(r) != null,
				() => converter.Compiled(r).Value == null);
		}

		[Fact]
		public void raise_error_on_missing_args() {
			var r = SequenceDataReader.Items(new { Value = (int?)null });
			var converter = ConverterFactory.Default.GetConverter<IDataReader, ValueRow<int>>(r);
			r.Read();
			Check.Exception<InvalidCastException>(() => converter.Compiled(r));
		}

		[Fact]
		public void into_existing() {
			var source = new { Item1 = 17 };
			var r = SequenceDataReader.Items(source);
			var into = ConverterFactory.Default.GetReadInto<ValueTuple<int>>(r);
			r.Read();
			var target = new ValueTuple<int>();
			into(r, ref target);
			Check.That(() => target.Item1 == source.Item1);
		}

		class ValueRow<T>
		{
			public ValueRow(T value) { this.Value = value; }
			public readonly T Value;
		}
	}
}
