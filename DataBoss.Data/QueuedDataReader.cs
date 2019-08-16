using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;

namespace DataBoss.Data
{
	static class Empty<T>
	{
		public static readonly T[] Array = new T[0];
	}

	public class QueuedDataReader<T> : IDataReader
	{
		readonly Func<IReadOnlyList<T>, IDataReader> executeBatch;
		readonly BlockingCollection<T> readQueue;
		readonly TimeSpan timeout;

		T[] batch;
		IDataReader current;

		public QueuedDataReader(Func<IReadOnlyList<T>, IDataReader> executeBatch, int batchSize = 1024, TimeSpan? timeout = null, int? maxQueue = null) {
			this.executeBatch = executeBatch;
			this.batch = new T[batchSize];
			this.timeout = timeout ?? TimeSpan.FromSeconds(1);
			this.readQueue = maxQueue.HasValue ? new BlockingCollection<T>(maxQueue.Value) : new BlockingCollection<T>();
		}

		IDataReader EnsureCurrent() => current ?? (current = executeBatch(Empty<T>.Array));

		public void Add(T item) => readQueue.Add(item);
		public void CompleteAdding() => readQueue.CompleteAdding();

		public object this[int i] => current[i];
		public object this[string name] => current[name];

		public int FieldCount => EnsureCurrent().FieldCount;

		public int Depth => current.Depth;
		public bool IsClosed => current.IsClosed;
		public int RecordsAffected => current.RecordsAffected;
		public void Close() => current?.Close();

		public void Dispose() {
			SetCurrent(null);
			batch = null;
		}

		public bool GetBoolean(int i) => current.GetBoolean(i);
		public byte GetByte(int i) => current.GetByte(i);
		public long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length) => current.GetBytes(i, fieldOffset, buffer, bufferoffset, length);
		public char GetChar(int i) => current.GetChar(i);
		public long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length) => current.GetChars(i, fieldoffset, buffer, bufferoffset, length);
		public IDataReader GetData(int i) => current.GetData(i);
		public string GetDataTypeName(int i) => current.GetDataTypeName(i);
		public DateTime GetDateTime(int i) => current.GetDateTime(i);
		public decimal GetDecimal(int i) => current.GetDecimal(i);
		public double GetDouble(int i) => current.GetDouble(i);
		public Type GetFieldType(int i) => current.GetFieldType(i);
		public float GetFloat(int i) => current.GetFloat(i);
		public Guid GetGuid(int i) => current.GetGuid(i);
		public short GetInt16(int i) => current.GetInt16(i);
		public int GetInt32(int i) => current.GetInt32(i);
		public long GetInt64(int i) => current.GetInt64(i);
		public string GetString(int i) => current.GetString(i);
		public object GetValue(int i) => current.GetValue(i);
		public int GetValues(object[] values) => current.GetValues(values);
		public bool IsDBNull(int i) => current.IsDBNull(i);

		public string GetName(int i) => EnsureCurrent().GetName(i);
		public int GetOrdinal(string name) => EnsureCurrent().GetOrdinal(name);
		public DataTable GetSchemaTable() => EnsureCurrent().GetSchemaTable();

		public bool NextResult() => false;

		public bool Read() {
			if (current != null && current.Read())
				if (current.Read())
					return true;
			return StartBatch() && Read();
		}

		bool StartBatch() {
			for (; ; ) {
				if (readQueue.Count == 0 && readQueue.IsAddingCompleted)
					return false;

				var n = 0;
				while (n < batch.Length && readQueue.TryTake(out var item, timeout))
					batch[n++] = item;
				if (n != 0) {
					SetCurrent(executeBatch(new ArraySegment<T>(batch, 0, n)));
					return true;
				}
			}
		}

		void SetCurrent(IDataReader next) {
			current?.Dispose();
			current = next;
		}
	}
}
