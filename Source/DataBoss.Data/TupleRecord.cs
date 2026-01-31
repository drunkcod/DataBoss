using System;
using System.Data;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;

namespace DataBoss.Data
{
	public abstract class TupleRecord : IDataRecord2
	{
		readonly DataReaderSchemaTable schema;

		protected TupleRecord(DataReaderSchemaTable schema) {
			this.schema = schema;
		}

		public void Dispose() { GC.SuppressFinalize(this); }

		public abstract int FieldCount { get; }
		public abstract T GetFieldValue<T>(int i);
		public abstract object GetValue(int i);
		internal abstract void Fill(IDataReader reader, int offset);

		internal void Fill(IDataReader reader) => Fill(reader, 0);

		public object this[int i] => GetValue(i);
		public object this[string name] => GetValue(GetOrdinal(name));

		public bool GetBoolean(int i) => GetFieldValue<bool>(i);
		public byte GetByte(int i) => GetFieldValue<byte>(i);
		public char GetChar(int i) => GetFieldValue<char>(i);
		public DateTime GetDateTime(int i) => GetFieldValue<DateTime>(i);
		public decimal GetDecimal(int i) => GetFieldValue<decimal>(i);
		public double GetDouble(int i) => GetFieldValue<double>(i);
		public float GetFloat(int i) => GetFieldValue<float>(i);
		public Guid GetGuid(int i) => GetFieldValue<Guid>(i);
		public short GetInt16(int i) => GetFieldValue<short>(i);
		public int GetInt32(int i) => GetFieldValue<int>(i);
		public long GetInt64(int i) => GetFieldValue<long>(i);
		public string GetString(int i) => GetFieldValue<string>(i);

		public long GetBytes(int i, long dataIndex, byte[] buffer, int bufferIndex, int length) {
			throw new NotImplementedException();
		}

		public long GetChars(int i, long dataIndex, char[] buffer, int bufferIndex, int length) {
			throw new NotImplementedException();
		}

		public string GetDataTypeName(int i) => schema[i].DataTypeName;
		[return: DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicProperties)]
		public Type GetFieldType(int i) => schema[i].DataType;
		public string GetName(int i) => schema[i].ColumnName;
		public int GetOrdinal(string name) => schema.GetOrdinal(name);

		public int GetValues(object[] values) {
			var n = Math.Min(FieldCount, values.Length);
			for (var i = 0; i != n; ++i)
				values[i] = GetValue(i);
			return n;
		}

		public abstract bool IsDBNull(int i);

		public IDataReader GetData(int i) => throw new NotSupportedException();
	}

	public sealed class TupleRecord<T0>(DataReaderSchemaTable schema) : TupleRecord(schema)
	{
		byte isDbNull;
		public T0 Item0;

		public override int FieldCount => 1;

		public override bool IsDBNull(int i) => i == 0 ? (isDbNull & 1) != 0 : throw new IndexOutOfRangeException();

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public override T GetFieldValue<T>(int i) {
			if (i == 0) {
				if ((isDbNull & 1) != 0) return default(T) == null ? default : throw new InvalidCastException();
				return (T)(object)Item0;
			}
			throw new IndexOutOfRangeException();
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public override object GetValue(int i) {
			if (i == 0) return (isDbNull & 1) != 0 ? DBNull.Value : (object)Item0;
			throw new IndexOutOfRangeException();
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		internal override void Fill(IDataReader reader, int offset) {
			if (reader.IsDBNull(offset)) {
				isDbNull = 1;
				Item0 = default;
			}
			else {
				isDbNull = 0;
				Item0 = reader.GetFieldValue<T0>(offset);
			}
		}
	}

	public sealed class TupleRecord<T0, T1>(DataReaderSchemaTable schema) : TupleRecord(schema)
	{
		byte isDbNull;
		public T0 Item0;
		public T1 Item1;

		public override int FieldCount => 2;

		public override bool IsDBNull(int i) => i < 2 ? (isDbNull & (1 << i)) != 0 : throw new IndexOutOfRangeException();

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public override T GetFieldValue<T>(int i) {
			if (i == 0) { if ((isDbNull & 1) != 0) return default(T) == null ? default : throw new InvalidCastException(); return (T)(object)Item0; }
			if (i == 1) { if ((isDbNull & 2) != 0) return default(T) == null ? default : throw new InvalidCastException(); return (T)(object)Item1; }
			throw new IndexOutOfRangeException();
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public override object GetValue(int i) {
			if (i == 0) return (isDbNull & 1) != 0 ? DBNull.Value : (object)Item0;
			if (i == 1) return (isDbNull & 2) != 0 ? DBNull.Value : (object)Item1;
			throw new IndexOutOfRangeException();
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		internal override void Fill(IDataReader reader, int offset) {
			if (reader.IsDBNull(offset + 0)) { isDbNull |= 1; Item0 = default; } else { isDbNull &= 254; Item0 = reader.GetFieldValue<T0>(offset + 0); }
			if (reader.IsDBNull(offset + 1)) { isDbNull |= 2; Item1 = default; } else { isDbNull &= 253; Item1 = reader.GetFieldValue<T1>(offset + 1); }
		}
	}

	public sealed class TupleRecord<T0, T1, T2>(DataReaderSchemaTable schema) : TupleRecord(schema)
	{
		byte isDbNull;
		public T0 Item0;
		public T1 Item1;
		public T2 Item2;

		public override int FieldCount => 3;

		public override bool IsDBNull(int i) => i < 3 ? (isDbNull & (1 << i)) != 0 : throw new IndexOutOfRangeException();

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public override T GetFieldValue<T>(int i) {
			if (i == 0) { if ((isDbNull & 1) != 0) return default(T) == null ? default : throw new InvalidCastException(); return (T)(object)Item0; }
			if (i == 1) { if ((isDbNull & 2) != 0) return default(T) == null ? default : throw new InvalidCastException(); return (T)(object)Item1; }
			if (i == 2) { if ((isDbNull & 4) != 0) return default(T) == null ? default : throw new InvalidCastException(); return (T)(object)Item2; }
			throw new IndexOutOfRangeException();
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public override object GetValue(int i) {
			if (i == 0) return (isDbNull & 1) != 0 ? DBNull.Value : (object)Item0;
			if (i == 1) return (isDbNull & 2) != 0 ? DBNull.Value : (object)Item1;
			if (i == 2) return (isDbNull & 4) != 0 ? DBNull.Value : (object)Item2;
			throw new IndexOutOfRangeException();
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		internal override void Fill(IDataReader reader, int offset) {
			if (reader.IsDBNull(offset + 0)) { isDbNull |= 1; Item0 = default; } else { isDbNull &= 254; Item0 = reader.GetFieldValue<T0>(offset + 0); }
			if (reader.IsDBNull(offset + 1)) { isDbNull |= 2; Item1 = default; } else { isDbNull &= 253; Item1 = reader.GetFieldValue<T1>(offset + 1); }
			if (reader.IsDBNull(offset + 2)) { isDbNull |= 4; Item2 = default; } else { isDbNull &= 251; Item2 = reader.GetFieldValue<T2>(offset + 2); }
		}
	}

	public sealed class TupleRecord<T0, T1, T2, T3>(DataReaderSchemaTable schema) : TupleRecord(schema)
	{
		byte isDbNull;
		public T0 Item0;
		public T1 Item1;
		public T2 Item2;
		public T3 Item3;

		public override int FieldCount => 4;

		public override bool IsDBNull(int i) => i < 4 ? (isDbNull & (1 << i)) != 0 : throw new IndexOutOfRangeException();

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public override T GetFieldValue<T>(int i) {
			if (i == 0) { if ((isDbNull & 1) != 0) return default(T) == null ? default : throw new InvalidCastException(); return (T)(object)Item0; }
			if (i == 1) { if ((isDbNull & 2) != 0) return default(T) == null ? default : throw new InvalidCastException(); return (T)(object)Item1; }
			if (i == 2) { if ((isDbNull & 4) != 0) return default(T) == null ? default : throw new InvalidCastException(); return (T)(object)Item2; }
			if (i == 3) { if ((isDbNull & 8) != 0) return default(T) == null ? default : throw new InvalidCastException(); return (T)(object)Item3; }
			throw new IndexOutOfRangeException();
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public override object GetValue(int i) {
			if (i == 0) return (isDbNull & 1) != 0 ? DBNull.Value : (object)Item0;
			if (i == 1) return (isDbNull & 2) != 0 ? DBNull.Value : (object)Item1;
			if (i == 2) return (isDbNull & 4) != 0 ? DBNull.Value : (object)Item2;
			if (i == 3) return (isDbNull & 8) != 0 ? DBNull.Value : (object)Item3;
			throw new IndexOutOfRangeException();
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		internal override void Fill(IDataReader reader, int offset) {
			if (reader.IsDBNull(offset + 0)) { isDbNull |= 1; Item0 = default; } else { isDbNull &= 254; Item0 = reader.GetFieldValue<T0>(offset + 0); }
			if (reader.IsDBNull(offset + 1)) { isDbNull |= 2; Item1 = default; } else { isDbNull &= 253; Item1 = reader.GetFieldValue<T1>(offset + 1); }
			if (reader.IsDBNull(offset + 2)) { isDbNull |= 4; Item2 = default; } else { isDbNull &= 251; Item2 = reader.GetFieldValue<T2>(offset + 2); }
			if (reader.IsDBNull(offset + 3)) { isDbNull |= 8; Item3 = default; } else { isDbNull &= 247; Item3 = reader.GetFieldValue<T3>(offset + 3); }
		}
	}

	public sealed class TupleRecord<T0, T1, T2, T3, T4>(DataReaderSchemaTable schema) : TupleRecord(schema)
	{
		byte isDbNull;
		public T0 Item0;
		public T1 Item1;
		public T2 Item2;
		public T3 Item3;
		public T4 Item4;

		public override int FieldCount => 5;

		public override bool IsDBNull(int i) => i < 5 ? (isDbNull & (1 << i)) != 0 : throw new IndexOutOfRangeException();

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public override T GetFieldValue<T>(int i) {
			if (i == 0) { if ((isDbNull & 1) != 0) return default(T) == null ? default : throw new InvalidCastException(); return (T)(object)Item0; }
			if (i == 1) { if ((isDbNull & 2) != 0) return default(T) == null ? default : throw new InvalidCastException(); return (T)(object)Item1; }
			if (i == 2) { if ((isDbNull & 4) != 0) return default(T) == null ? default : throw new InvalidCastException(); return (T)(object)Item2; }
			if (i == 3) { if ((isDbNull & 8) != 0) return default(T) == null ? default : throw new InvalidCastException(); return (T)(object)Item3; }
			if (i == 4) { if ((isDbNull & 16) != 0) return default(T) == null ? default : throw new InvalidCastException(); return (T)(object)Item4; }
			throw new IndexOutOfRangeException();
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public override object GetValue(int i) {
			if (i == 0) return (isDbNull & 1) != 0 ? DBNull.Value : (object)Item0;
			if (i == 1) return (isDbNull & 2) != 0 ? DBNull.Value : (object)Item1;
			if (i == 2) return (isDbNull & 4) != 0 ? DBNull.Value : (object)Item2;
			if (i == 3) return (isDbNull & 8) != 0 ? DBNull.Value : (object)Item3;
			if (i == 4) return (isDbNull & 16) != 0 ? DBNull.Value : (object)Item4;
			throw new IndexOutOfRangeException();
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		internal override void Fill(IDataReader reader, int offset) {
			if (reader.IsDBNull(offset + 0)) { isDbNull |= 1; Item0 = default; } else { isDbNull &= 254; Item0 = reader.GetFieldValue<T0>(offset + 0); }
			if (reader.IsDBNull(offset + 1)) { isDbNull |= 2; Item1 = default; } else { isDbNull &= 253; Item1 = reader.GetFieldValue<T1>(offset + 1); }
			if (reader.IsDBNull(offset + 2)) { isDbNull |= 4; Item2 = default; } else { isDbNull &= 251; Item2 = reader.GetFieldValue<T2>(offset + 2); }
			if (reader.IsDBNull(offset + 3)) { isDbNull |= 8; Item3 = default; } else { isDbNull &= 247; Item3 = reader.GetFieldValue<T3>(offset + 3); }
			if (reader.IsDBNull(offset + 4)) { isDbNull |= 16; Item4 = default; } else { isDbNull &= 239; Item4 = reader.GetFieldValue<T4>(offset + 4); }
		}
	}

	public sealed class TupleRecord<T0, T1, T2, T3, T4, T5>(DataReaderSchemaTable schema) : TupleRecord(schema)
	{
		byte isDbNull;
		public T0 Item0;
		public T1 Item1;
		public T2 Item2;
		public T3 Item3;
		public T4 Item4;
		public T5 Item5;

		public override int FieldCount => 6;

		public override bool IsDBNull(int i) => i < 6 ? (isDbNull & (1 << i)) != 0 : throw new IndexOutOfRangeException();

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public override T GetFieldValue<T>(int i) {
			if (i == 0) { if ((isDbNull & 1) != 0) return default(T) == null ? default : throw new InvalidCastException(); return (T)(object)Item0; }
			if (i == 1) { if ((isDbNull & 2) != 0) return default(T) == null ? default : throw new InvalidCastException(); return (T)(object)Item1; }
			if (i == 2) { if ((isDbNull & 4) != 0) return default(T) == null ? default : throw new InvalidCastException(); return (T)(object)Item2; }
			if (i == 3) { if ((isDbNull & 8) != 0) return default(T) == null ? default : throw new InvalidCastException(); return (T)(object)Item3; }
			if (i == 4) { if ((isDbNull & 16) != 0) return default(T) == null ? default : throw new InvalidCastException(); return (T)(object)Item4; }
			if (i == 5) { if ((isDbNull & 32) != 0) return default(T) == null ? default : throw new InvalidCastException(); return (T)(object)Item5; }
			throw new IndexOutOfRangeException();
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public override object GetValue(int i) {
			if (i == 0) return (isDbNull & 1) != 0 ? DBNull.Value : (object)Item0;
			if (i == 1) return (isDbNull & 2) != 0 ? DBNull.Value : (object)Item1;
			if (i == 2) return (isDbNull & 4) != 0 ? DBNull.Value : (object)Item2;
			if (i == 3) return (isDbNull & 8) != 0 ? DBNull.Value : (object)Item3;
			if (i == 4) return (isDbNull & 16) != 0 ? DBNull.Value : (object)Item4;
			if (i == 5) return (isDbNull & 32) != 0 ? DBNull.Value : (object)Item5;
			throw new IndexOutOfRangeException();
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		internal override void Fill(IDataReader reader, int offset) {
			if (reader.IsDBNull(offset + 0)) { isDbNull |= 1; Item0 = default; } else { isDbNull &= 254; Item0 = reader.GetFieldValue<T0>(offset + 0); }
			if (reader.IsDBNull(offset + 1)) { isDbNull |= 2; Item1 = default; } else { isDbNull &= 253; Item1 = reader.GetFieldValue<T1>(offset + 1); }
			if (reader.IsDBNull(offset + 2)) { isDbNull |= 4; Item2 = default; } else { isDbNull &= 251; Item2 = reader.GetFieldValue<T2>(offset + 2); }
			if (reader.IsDBNull(offset + 3)) { isDbNull |= 8; Item3 = default; } else { isDbNull &= 247; Item3 = reader.GetFieldValue<T3>(offset + 3); }
			if (reader.IsDBNull(offset + 4)) { isDbNull |= 16; Item4 = default; } else { isDbNull &= 239; Item4 = reader.GetFieldValue<T4>(offset + 4); }
			if (reader.IsDBNull(offset + 5)) { isDbNull |= 32; Item5 = default; } else { isDbNull &= 223; Item5 = reader.GetFieldValue<T5>(offset + 5); }
		}
	}

	public sealed class TupleRecord<T0, T1, T2, T3, T4, T5, T6>(DataReaderSchemaTable schema) : TupleRecord(schema)
	{
		byte isDbNull;
		public T0 Item0;
		public T1 Item1;
		public T2 Item2;
		public T3 Item3;
		public T4 Item4;
		public T5 Item5;
		public T6 Item6;

		public override int FieldCount => 7;

		public override bool IsDBNull(int i) => i < 7 ? (isDbNull & (1 << i)) != 0 : throw new IndexOutOfRangeException();

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public override T GetFieldValue<T>(int i) {
			if (i == 0) { if ((isDbNull & 1) != 0) return default(T) == null ? default : throw new InvalidCastException(); return (T)(object)Item0; }
			if (i == 1) { if ((isDbNull & 2) != 0) return default(T) == null ? default : throw new InvalidCastException(); return (T)(object)Item1; }
			if (i == 2) { if ((isDbNull & 4) != 0) return default(T) == null ? default : throw new InvalidCastException(); return (T)(object)Item2; }
			if (i == 3) { if ((isDbNull & 8) != 0) return default(T) == null ? default : throw new InvalidCastException(); return (T)(object)Item3; }
			if (i == 4) { if ((isDbNull & 16) != 0) return default(T) == null ? default : throw new InvalidCastException(); return (T)(object)Item4; }
			if (i == 5) { if ((isDbNull & 32) != 0) return default(T) == null ? default : throw new InvalidCastException(); return (T)(object)Item5; }
			if (i == 6) { if ((isDbNull & 64) != 0) return default(T) == null ? default : throw new InvalidCastException(); return (T)(object)Item6; }
			throw new IndexOutOfRangeException();
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public override object GetValue(int i) {
			if (i == 0) return (isDbNull & 1) != 0 ? DBNull.Value : (object)Item0;
			if (i == 1) return (isDbNull & 2) != 0 ? DBNull.Value : (object)Item1;
			if (i == 2) return (isDbNull & 4) != 0 ? DBNull.Value : (object)Item2;
			if (i == 3) return (isDbNull & 8) != 0 ? DBNull.Value : (object)Item3;
			if (i == 4) return (isDbNull & 16) != 0 ? DBNull.Value : (object)Item4;
			if (i == 5) return (isDbNull & 32) != 0 ? DBNull.Value : (object)Item5;
			if (i == 6) return (isDbNull & 64) != 0 ? DBNull.Value : (object)Item6;
			throw new IndexOutOfRangeException();
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		internal override void Fill(IDataReader reader, int offset) {
			if (reader.IsDBNull(offset + 0)) { isDbNull |= 1; Item0 = default; } else { isDbNull &= 254; Item0 = reader.GetFieldValue<T0>(offset + 0); }
			if (reader.IsDBNull(offset + 1)) { isDbNull |= 2; Item1 = default; } else { isDbNull &= 253; Item1 = reader.GetFieldValue<T1>(offset + 1); }
			if (reader.IsDBNull(offset + 2)) { isDbNull |= 4; Item2 = default; } else { isDbNull &= 251; Item2 = reader.GetFieldValue<T2>(offset + 2); }
			if (reader.IsDBNull(offset + 3)) { isDbNull |= 8; Item3 = default; } else { isDbNull &= 247; Item3 = reader.GetFieldValue<T3>(offset + 3); }
			if (reader.IsDBNull(offset + 4)) { isDbNull |= 16; Item4 = default; } else { isDbNull &= 239; Item4 = reader.GetFieldValue<T4>(offset + 4); }
			if (reader.IsDBNull(offset + 5)) { isDbNull |= 32; Item5 = default; } else { isDbNull &= 223; Item5 = reader.GetFieldValue<T5>(offset + 5); }
			if (reader.IsDBNull(offset + 6)) { isDbNull |= 64; Item6 = default; } else { isDbNull &= 191; Item6 = reader.GetFieldValue<T6>(offset + 6); }
		}
	}

	public sealed class TupleRecord<T0, T1, T2, T3, T4, T5, T6, TRest>(DataReaderSchemaTable schema, TRest rest) : TupleRecord(schema)
		where TRest : TupleRecord
	{
		byte isDbNull;
		public T0 Item0;
		public T1 Item1;
		public T2 Item2;
		public T3 Item3;
		public T4 Item4;
		public T5 Item5;
		public T6 Item6;
		public TRest Rest = rest;

		public override int FieldCount => 7 + Rest.FieldCount;

		public override bool IsDBNull(int i) => i < 7 ? (isDbNull & (1 << i)) != 0 : Rest.IsDBNull(i - 7);

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public override T GetFieldValue<T>(int i) {
			if (i < 7) {
				if (i == 0) { if ((isDbNull & 1) != 0) return default(T) == null ? default : throw new InvalidCastException(); return (T)(object)Item0; }
				if (i == 1) { if ((isDbNull & 2) != 0) return default(T) == null ? default : throw new InvalidCastException(); return (T)(object)Item1; }
				if (i == 2) { if ((isDbNull & 4) != 0) return default(T) == null ? default : throw new InvalidCastException(); return (T)(object)Item2; }
				if (i == 3) { if ((isDbNull & 8) != 0) return default(T) == null ? default : throw new InvalidCastException(); return (T)(object)Item3; }
				if (i == 4) { if ((isDbNull & 16) != 0) return default(T) == null ? default : throw new InvalidCastException(); return (T)(object)Item4; }
				if (i == 5) { if ((isDbNull & 32) != 0) return default(T) == null ? default : throw new InvalidCastException(); return (T)(object)Item5; }
				if (i == 6) { if ((isDbNull & 64) != 0) return default(T) == null ? default : throw new InvalidCastException(); return (T)(object)Item6; }
			}
			return Rest.GetFieldValue<T>(i - 7);
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public override object GetValue(int i) {
			if (i < 7) {
				if (i == 0) return (isDbNull & 1) != 0 ? DBNull.Value : (object)Item0;
				if (i == 1) return (isDbNull & 2) != 0 ? DBNull.Value : (object)Item1;
				if (i == 2) return (isDbNull & 4) != 0 ? DBNull.Value : (object)Item2;
				if (i == 3) return (isDbNull & 8) != 0 ? DBNull.Value : (object)Item3;
				if (i == 4) return (isDbNull & 16) != 0 ? DBNull.Value : (object)Item4;
				if (i == 5) return (isDbNull & 32) != 0 ? DBNull.Value : (object)Item5;
				if (i == 6) return (isDbNull & 64) != 0 ? DBNull.Value : (object)Item6;
			}
			return Rest.GetValue(i - 7);
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		internal override void Fill(IDataReader reader, int offset) {
			if (reader.IsDBNull(offset + 0)) { isDbNull |= 1; Item0 = default; } else { isDbNull &= 254; Item0 = reader.GetFieldValue<T0>(offset + 0); }
			if (reader.IsDBNull(offset + 1)) { isDbNull |= 2; Item1 = default; } else { isDbNull &= 253; Item1 = reader.GetFieldValue<T1>(offset + 1); }
			if (reader.IsDBNull(offset + 2)) { isDbNull |= 4; Item2 = default; } else { isDbNull &= 251; Item2 = reader.GetFieldValue<T2>(offset + 2); }
			if (reader.IsDBNull(offset + 3)) { isDbNull |= 8; Item3 = default; } else { isDbNull &= 247; Item3 = reader.GetFieldValue<T3>(offset + 3); }
			if (reader.IsDBNull(offset + 4)) { isDbNull |= 16; Item4 = default; } else { isDbNull &= 239; Item4 = reader.GetFieldValue<T4>(offset + 4); }
			if (reader.IsDBNull(offset + 5)) { isDbNull |= 32; Item5 = default; } else { isDbNull &= 223; Item5 = reader.GetFieldValue<T5>(offset + 5); }
			if (reader.IsDBNull(offset + 6)) { isDbNull |= 64; Item6 = default; } else { isDbNull &= 191; Item6 = reader.GetFieldValue<T6>(offset + 6); }

			Rest.Fill(reader, offset + 7);
		}
	}

	public class TupleRecordReader(IDataReader reader) : IDataRecordReader
	{
		static Func<DataReaderSchemaTable, TupleRecord> GetRecordFactory(IDataReader reader) {
			var width = reader.FieldCount;
			var ts = new Type[width];
			for (var i = 0; i != width; ++i)
				ts[i] = reader.GetFieldType(i);

			var schemaParam = Expression.Parameter(typeof(DataReaderSchemaTable));
			return Expression.Lambda<Func<DataReaderSchemaTable, TupleRecord>>(
				CreateFactoryExpression(ts, schemaParam), schemaParam).Compile();
		}

		static NewExpression CreateFactoryExpression(Type[] types, ParameterExpression schemaParam) {
			if (types.Length <= 7) {
				var t = types.Length switch {
					1 => typeof(TupleRecord<>),
					2 => typeof(TupleRecord<,>),
					3 => typeof(TupleRecord<,,>),
					4 => typeof(TupleRecord<,,,>),
					5 => typeof(TupleRecord<,,,,>),
					6 => typeof(TupleRecord<,,,,,>),
					7 => typeof(TupleRecord<,,,,,,>),
					_ => throw new NotSupportedException(),
				};
				return Expression.New(t.MakeGenericType(types).GetConstructor([typeof(DataReaderSchemaTable)]), schemaParam);
			}

			var head = types[..7];
			var tail = types[7..];
			var restExpr = CreateFactoryExpression(tail, schemaParam);

			var paramsType = head.Concat(new[] { restExpr.Type }).ToArray();
			var tupleType = typeof(TupleRecord<,,,,,,,>).MakeGenericType(paramsType);

			return Expression.New(
				tupleType.GetConstructor([typeof(DataReaderSchemaTable), restExpr.Type]),
				schemaParam,
				restExpr);
		}

		readonly IDataReader reader = reader;
		readonly DataReaderSchemaTable schema = reader.GetDataReaderSchemaTable();
		readonly Func<DataReaderSchemaTable, TupleRecord> newEmptyRecord = GetRecordFactory(reader);

		public IDataRecord2 GetRecord() {
			var r = newEmptyRecord(schema);
			r.Fill(reader);
			return r;
		}

		public bool Read() => reader.Read();
	}
}