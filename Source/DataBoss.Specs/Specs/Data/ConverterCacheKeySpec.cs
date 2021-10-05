using System;
using System.Collections.Generic;
using System.Data;
using System.Linq.Expressions;
using CheckThat;
using Xunit;

namespace DataBoss.Data
{
	public class ConverterCacheKeySpec
	{
		[Fact]
		public void ctor_key() {
			Check.That(
				() => KeyString<Func<int, KeyValuePair<int, int>>>(
					SequenceDataReader.Items(new { x = 1 }), 
					x => new KeyValuePair<int, int>(x, x)) == "System.Data.IDataReader(System.Int32 $0)⇒.ctor($0, $0)",
				() => KeyString<Func<string, int, KeyValuePair<int, string>>>(
					SequenceDataReader.Items(new { Key = "key", Id = 1 }),
					(key, id) => new KeyValuePair<int, string>(id, key)) == "System.Data.IDataReader(System.String $0, System.Int32 $1)⇒.ctor($1, $0)");
		}

		static string KeyString<T>(IDataReader r, Expression<T> expr) where T : Delegate =>
			ConverterCacheKey.TryCreate(r, expr, out var key) ? key.ToString() : throw new InvalidOperationException();
	}

}
