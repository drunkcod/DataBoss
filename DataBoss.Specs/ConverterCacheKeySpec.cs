using System;
using System.Collections.Generic;
using System.Data;
using System.Linq.Expressions;
using Cone;
using DataBoss.Data;

namespace DataBoss.Specs
{
	[Describe(typeof(ConverterCacheKey))]
	public class ConverterCacheKeySpec
	{
		public void ctor_key() {
			IDataReader r = SequenceDataReader.Create(new[] { new { x = 1 } });
			var created = ConverterCacheKey.TryCreate(r, Expr<int, KeyValuePair<int, int>>(x => new KeyValuePair<int, int>(x, x)), out var key);
			Check.That(() => created);
			Check.That(() => key.ToString() == "System.Data.IDataReader(System.Int32)->.ctor(System.Int32 _0, System.Int32 _0)");
		}

		static Expression<Func<TArg0, T>> Expr<TArg0, T>(Expression<Func<TArg0, T>> e) => e;
	}

}
