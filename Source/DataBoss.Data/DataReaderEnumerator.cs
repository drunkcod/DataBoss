using System;
using System.Collections;
using System.Data;

namespace DataBoss.Data
{
	public class DataReaderEnumerator : IEnumerator
	{
		readonly IDataReader reader;

		public DataReaderEnumerator(IDataReader reader) {
			this.reader = reader;
		}

		public object Current => (IDataRecord)reader;

		public bool MoveNext() => reader.Read();
		public void Reset() => throw new NotSupportedException();
	}
}
