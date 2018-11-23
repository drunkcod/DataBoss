using Cone;
using DataBoss.Data;
using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq.Expressions;

namespace DataBoss.Specs
{
	[Describe(typeof(ConverterFactory))]
	public class ConverterFactorySpec
	{
		public void reuse_converter_for_matching_field_map() {
			var factory = new ConverterFactory(new ConverterCollection());

			var map = new FieldMap();
			map.Add("key", 0, typeof(int), null, true);
			map.Add("value", 1, typeof(string), null, true);

			var reader0 = SequenceDataReader.Create(new[]{ new { key = 0, value = "0"}}, x => x.MapAll());
			var reader1 = SequenceDataReader.Create(new[] { 1 }, x => {
				x.Map("key", item => item);
				x.Map("value", item => item.ToString());
			});

			Check.That(() => factory.GetConverter<IDataReader, KeyValuePair<int, string>>(reader0).Compiled == factory.GetConverter<IDataReader, KeyValuePair<int, string>>(reader1).Compiled);
		}

		public void factory_expression_converter() {
			var factory = new ConverterFactory(new ConverterCollection());
			var reader = SequenceDataReader.Create(new[] { new { key = 1, } }, x => x.MapAll());
			reader.Read();
			Check.With(() => factory.GetConverter(reader, (int key) => new KeyValuePair<int, string>(key, key.ToString())))
				.That(
					converter => converter.Compiled(reader).Key == reader.GetInt32(0), 
					converter => converter.Compiled(reader).Value == reader.GetInt32(0).ToString());
		}

		public void factory_expression_ctor_reuse() {
			var factory = new ConverterFactory(new ConverterCollection(), new ConcurrentConverterCache());
			var reader = SequenceDataReader.Create(new[] { new { x = 1, } }, x => x.MapAll());
			Check.That(() => Equals(
				factory.GetConverter<IDataReader, int, KeyValuePair<int, int>>(reader, x => new KeyValuePair<int, int>(x, x)),
				factory.GetConverter<IDataReader, int, KeyValuePair<int, int>>(reader, x => new KeyValuePair<int, int>(x, x))));
		}
	}

	[Describe(typeof(ConverterCacheKey))]
	public class ConverterCacheKeySpec
	{
		public void ctor_key() {
			var created = ConverterCacheKey.TryCreate(typeof(IDataReader), Expr<int, KeyValuePair<int, int>>(x => new KeyValuePair<int, int>(x, x)), out var key);
			Check.That(() => created);
			Check.That(() => key.ToString() == "System.Data.IDataReader -> .ctor(System.Int32 _0, System.Int32 _0)");
		}

		static Expression<Func<TArg0, T>> Expr<TArg0, T>(Expression<Func<TArg0, T>> e) => e;
	}

}
