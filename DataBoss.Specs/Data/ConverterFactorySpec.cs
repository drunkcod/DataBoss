using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
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

		class ValueRow<T>
		{
			public ValueRow(T value) { this.Value = value; }
			public readonly T Value;
		}
	}
}
