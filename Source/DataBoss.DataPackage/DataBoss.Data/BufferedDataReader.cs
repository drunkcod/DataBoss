using System;
using System.Buffers;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Channels;
using System.Threading.Tasks;
using DataBoss.Threading.Channels;

namespace DataBoss.Data
{
	class BufferedDataReader : IDataReader
	{
		readonly IDataReader reader;
		readonly IDataRecordReader records;
		readonly Channel<IEnumerator<IDataRecord2>> buffer;
		readonly Task recordReader;
		readonly int chunkSize;
		IEnumerator<IDataRecord2> items;
		bool hasRead = false;

		public BufferedDataReader(IDataReader reader, int chunkSize = 1024, int? maxBacklog = null) {
			this.reader = reader;
			this.records = reader.AsDataRecordReader();
			this.items = Enumerable.Empty<IDataRecord2>().GetEnumerator();
			this.chunkSize = chunkSize;

			if (maxBacklog.HasValue)
				this.buffer = Channel.CreateBounded<IEnumerator<IDataRecord2>>(new BoundedChannelOptions(maxBacklog.Value) {
					SingleReader = true,
					SingleWriter = true,
				});
			else
				this.buffer = Channel.CreateUnbounded<IEnumerator<IDataRecord2>>(new UnboundedChannelOptions {
					SingleReader = true,
					SingleWriter = true,
				});

			this.recordReader = Task.Factory.StartNew(ReadRecords, TaskCreationOptions.LongRunning);
		}

		void ReadRecords() {
			var w = buffer.Writer;
			try {
				var chunk = NewChunk(chunkSize);
				while (records.Read()) {
					chunk.Add(records.GetRecord());
					if (chunk.Count == chunk.Capacity) {
						w.Write(chunk.GetEnumerator());
						chunk = NewChunk(chunkSize);
					}
				}
				if (chunk.Count != 0)
					w.Write(chunk.GetEnumerator());
			}
			catch (Exception e) {
				w.Write(new ErrorEnumerable<IDataRecord2>(e).GetEnumerator());
			}
			finally {
				w.Complete();
			}
		}

		class Chunk<T> : IDisposable
		{
			T[] items;
			int count = 0;

			public Chunk(int size) {
				this.items = ArrayPool<T>.Shared.Rent(size);
			}

			public struct Enumerator(Chunk<T> chunk) : IEnumerator<T>
			{
				int i = -1;
				public T Current => chunk.items[i];

				object IEnumerator.Current => Current;

				public void Dispose() => chunk.Dispose();

				public bool MoveNext() {
					if (++i >= chunk.Count) return false;
					return true;

				}

				public void Reset() {
					i = 0;
				}
			}

			public int Capacity => items.Length;
			public int Count => count;

			public void Add(T item) {
				items[count++] = item;
			}


			public void Dispose() {
				if (items is not null) {
					ArrayPool<T>.Shared.Return(items);
				}
				items = null;
			}

			public Enumerator GetEnumerator() => new(this);
		}

		static Chunk<IDataRecord2> NewChunk(int size) => new(size);

		IDataRecord Current => items.Current;

		public object this[int i] => Current[i];
		public object this[string name] => Current[name];

		public int Depth => reader.Depth;
		public bool IsClosed => reader.IsClosed;
		public int RecordsAffected => reader.RecordsAffected;
		public int FieldCount => reader.FieldCount;

		public bool NextResult() => false;

		public bool Read() {
			if (hasRead) items.Current.Dispose();
			while (!items.MoveNext()) {
				var r = buffer.Reader;
				if (!r.WaitToRead() || !r.TryRead(out var next))
					return false;
				else {
					items.Dispose();
					items = next;
				}
			}
			hasRead = true;
			return true;
		}

		public void Close() => reader.Close();
		public void Dispose() => reader.Dispose();

		public DataTable GetSchemaTable() => reader.GetSchemaTable();
		public string GetName(int i) => reader.GetName(i);
		public int GetOrdinal(string name) => reader.GetOrdinal(name);
		public Type GetFieldType(int i) => reader.GetFieldType(i);

		public bool IsDBNull(int i) => Current.IsDBNull(i);
		public bool GetBoolean(int i) => Current.GetBoolean(i);
		public byte GetByte(int i) => Current.GetByte(i);
		public long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length) => Current.GetBytes(i, fieldOffset, buffer, bufferoffset, length);
		public char GetChar(int i) => Current.GetChar(i);
		public long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length) => Current.GetChars(i, fieldoffset, buffer, bufferoffset, length);
		public IDataReader GetData(int i) => Current.GetData(i);
		public string GetDataTypeName(int i) => Current.GetDataTypeName(i);
		public DateTime GetDateTime(int i) => Current.GetDateTime(i);
		public decimal GetDecimal(int i) => Current.GetDecimal(i);
		public double GetDouble(int i) => Current.GetDouble(i);
		public float GetFloat(int i) => Current.GetFloat(i);
		public Guid GetGuid(int i) => Current.GetGuid(i);
		public short GetInt16(int i) => Current.GetInt16(i);
		public int GetInt32(int i) => Current.GetInt32(i);
		public long GetInt64(int i) => Current.GetInt64(i);
		public string GetString(int i) => Current.GetString(i);
		public object GetValue(int i) => Current.GetValue(i);
		public int GetValues(object[] values) => Current.GetValues(values);
	}
}
