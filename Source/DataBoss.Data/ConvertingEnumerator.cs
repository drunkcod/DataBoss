using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;

namespace DataBoss.Data
{
	sealed class ConvertingEnumerator<TReader, T> : IEnumerator<T> where TReader : IDataReader
	{
		readonly TReader reader;
		readonly Func<TReader, T> convert;
		readonly bool leaveOpen;

		public ConvertingEnumerator(TReader reader, Func<TReader, T> convert, bool leaveOpen = false) {
			this.reader = reader;
			this.convert = convert;
			this.leaveOpen = leaveOpen;
		}

		public T Current { get; private set; }
		object IEnumerator.Current => Current;

		void IDisposable.Dispose() {
			if(!leaveOpen)
				reader.Dispose();
		}

		public bool MoveNext() {
			if (!reader.IsClosed && reader.Read()) {
				Current = convert(reader);
				return true;
			} else {
				if(!leaveOpen)
					reader.Close();
				Current = default;
				return false;
			}
		}

		public void Reset() => throw new NotSupportedException();
	}
}