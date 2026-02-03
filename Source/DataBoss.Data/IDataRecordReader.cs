using System;
using System.Data;

namespace DataBoss.Data;

public interface IDataRecord2 : IDataRecord, IDisposable
{ }

public interface IDataRecordReader
{
	bool Read();
	IDataRecord2 GetRecord();
}

public static class DataRecordReader
{
	public static Func<IDataReader, IDataRecordReader> Factory { get; set; } = reader => new ObjectDataRecordReader(reader);

	public static IDataRecordReader AsDataRecordReader(this IDataReader reader) =>
		reader is IDataRecordReader records ? records : Factory(reader);
}
