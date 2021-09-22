using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;

namespace DataBoss.Data
{
	public static class DataPackageDataReaderExtensions
	{
		public static IDataReader AsBuffered(this IDataReader reader) => new BufferedDataReader(reader);
		public static IDataReader Concat(this IDataReader first, IDataReader second) => ConcatDataReader.Create(new[] { first, second });
		public static IDataReader Concat<T>(this IDataReader first, IEnumerable<T> second) => first.Concat(second.ToDataReader());

		public static IDataReader Where(this IDataReader source, Func<IDataRecord, bool> predicate) => new WhereDataReader(source.AsDbDataReader(), predicate);
		public static IDataReader Where(this DbDataReader source, Func<IDataRecord, bool> predicate) => new WhereDataReader(source, predicate);
	}
}
