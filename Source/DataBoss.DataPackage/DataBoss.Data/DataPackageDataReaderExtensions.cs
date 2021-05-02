using System.Data;

namespace DataBoss.Data
{
	public static class DataPackageDataReaderExtensions
	{
		public static IDataReader AsBuffered(this IDataReader reader) => new BufferedDataReader(reader);
		public static IDataReader Concat(this IDataReader first, IDataReader second) => ConcatDataReader.Create(new[] { first, second });
	}
}
