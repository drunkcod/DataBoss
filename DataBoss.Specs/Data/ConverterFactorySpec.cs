using System;
using System.Data;
using Cone;
using DataBoss.Data;

namespace DataBoss.Specs.Data
{
	[Describe(typeof(ConverterFactory))]
	public class ConverterFactorySpec
	{
		public void ctor_dbnull_reference_arg() {
			var r = SequenceDataReader.Items(new { Value = (string)null });
			var converter = ConverterFactory.Default.GetConverter<IDataReader, ValueRow<string>>(r);
			r.Read();
			Check.That(
				() => converter.Compiled(r) != null,
				() => converter.Compiled(r).Value == null);
		}

		public void ctor_dbnull_nullable_arg() {
			var r = SequenceDataReader.Items(new { Value = (int?)null });
			var converter = ConverterFactory.Default.GetConverter<IDataReader, ValueRow<int?>>(r);
			r.Read();
			Check.That(
				() => converter.Compiled(r) != null,
				() => converter.Compiled(r).Value == null);
		}

		public void raise_error_on_missing_args() {
			var r = SequenceDataReader.Items(new { Value = (int?)null });
			var converter = ConverterFactory.Default.GetConverter<IDataReader, ValueRow<int>>(r);
			r.Read();
			Check.Exception<InvalidCastException>(() => converter.Compiled(r));
		}

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
