using System;
using System.Buffers;
using System.Data;

namespace DataBoss.Data
{
	public interface IDataRecord2 : IDataRecord, IDisposable
	{ }

	public interface IDataRecordReader
	{
		bool Read();
		IDataRecord2 GetRecord();
	}

	public static class DataRecordReaderExtensions
	{
		public static IDataRecordReader AsDataRecordReader(this IDataReader reader) =>
			reader is IDataRecordReader records ? records : new ObjectDataRecordReader(reader);
	}
}
