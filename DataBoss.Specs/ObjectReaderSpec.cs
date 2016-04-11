using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using Cone;
using Cone.Core;

namespace DataBoss.Specs
{
	[Describe(typeof(ObjectReader))]
	public class ObjectReaderSpec
	{
		class SimpleDataReader : IDataReader
		{
			readonly string[] names;
			
			readonly List<object[]> records = new List<object[]>();
			int currentRecord;
			public SimpleDataReader(params string[] names) {
				this.names = names;
			}

			public void Add(params object[] record) {
				if(record.Length != names.Length)
					throw new InvalidOperationException("Invalid record length");
				records.Add(record);
			}

			public int Count => records.Count;
			public int FieldCount => names.Length;

			public bool Read() {
				if(currentRecord == records.Count)
					return false;
				++currentRecord;
				return true;
			}
			public string GetName(int i) { return names[i]; }
			public object GetValue(int i) { return records[currentRecord - 1][i]; }

			public void Dispose() { }

			public string GetDataTypeName(int i) { throw new NotImplementedException(); }
			public Type GetFieldType(int i) { throw new NotImplementedException(); }

			public int GetValues(object[] values) { throw new NotImplementedException(); }

			public int GetOrdinal(string name) { throw new NotImplementedException(); }

			public bool GetBoolean(int i) { throw new NotImplementedException(); }

			public byte GetByte(int i) { throw new NotImplementedException(); }

			public long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length) { throw new NotImplementedException(); }

			public char GetChar(int i) { throw new NotImplementedException(); }

			public long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length) { throw new NotImplementedException(); }

			public Guid GetGuid(int i) { throw new NotImplementedException(); }

			public short GetInt16(int i) { throw new NotImplementedException(); }

			public int GetInt32(int i) => (int)GetValue(i);

			public long GetInt64(int i) => (long)GetValue(i);

			public float GetFloat(int i) => (float)GetValue(i);

			public double GetDouble(int i) { throw new NotImplementedException(); }

			public string GetString(int i) => (string)GetValue(i);

			public decimal GetDecimal(int i) { throw new NotImplementedException(); }

			public DateTime GetDateTime(int i) { throw new NotImplementedException(); }

			public IDataReader GetData(int i) { throw new NotImplementedException(); }

			public bool IsDBNull(int i) => GetValue(i) == null;

			object IDataRecord.this[int i]
			{
				get { throw new NotImplementedException(); }
			}

			object IDataRecord.this[string name]
			{
				get { throw new NotImplementedException(); }
			}

			public void Close() { throw new NotImplementedException(); }

			public DataTable GetSchemaTable() { throw new NotImplementedException(); }

			public bool NextResult() { throw new NotImplementedException(); }

			public int Depth { get { throw new NotImplementedException(); } }
			public bool IsClosed { get { throw new NotImplementedException(); } }
			public int RecordsAffected { get { throw new NotImplementedException(); } }
		}

		public void converts_all_rows() {
			var source = new SimpleDataReader("Id", "Context", "Name");
			source.Add(1L, "", "First");
			source.Add(2L, "", "Second");
			var reader = new ObjectReader();
			Check.That(() => reader.Read<DataBossMigrationInfo>(source).Count() == source.Count);
		}

		public void reads_public_fields() {
			var source = new SimpleDataReader("Id", "Context", "Name");
			source.Add(1L, "", "First");
			var reader = new ObjectReader();
			var read = reader.Read<DataBossMigrationInfo>(source).Single();
			Check.That(
				() => read.Id == 1,
				() => read.Context == "",
				() => read.Name == "First");
		}

		public void converter_fills_public_fields() {
			var source = new SimpleDataReader("Id", "Context", "Name");
			var formatter = new ExpressionFormatter(GetType());
			Check.That(() => formatter.Format(ObjectReader.MakeConverter<DataBossMigrationInfo>(source)) == "x => new DataBossMigrationInfo { Id = x.GetInt64(0), Context = x.IsDBNull(1) ? default(string) : x.GetString(1), Name = x.IsDBNull(2) ? default(string) : x.GetString(2) }");
		}

		class ValueRow<T> { public T Value; }

		public void supports_float_field() {
			var source = new SimpleDataReader("Value");
			var expected = new ValueRow<float> { Value = 3.14f };
			source.Add(expected.Value);
			var reader = new ObjectReader();
			var rows = (ValueRow<float>[])Check.That(() => reader.Read<ValueRow<float>>(source).ToArray() != null);
			Check.That(
				() => rows.Length == 1,
				() => rows[0].Value == expected.Value);
		}

		public void supports_binary_field() {
			var source = new SimpleDataReader("Value");
			var expected = new ValueRow<byte[]> { Value = Encoding.UTF8.GetBytes("Hello World!") };
			source.Add(expected.Value);
			var reader = new ObjectReader();
			var rows = (ValueRow<byte[]>[])Check.That(() => reader.Read<ValueRow<byte[]>>(source).ToArray() != null);
			Check.That(
				() => rows.Length == 1,
				() => rows[0].Value == expected.Value);
		}

		public void supports_nested_fields() {
			var source = new SimpleDataReader("Value.Value");
			var expected = new ValueRow<ValueRow<int>> { Value = new ValueRow<int> { Value = 42 } };
			source.Add(expected.Value.Value);
			var reader = new ObjectReader();
			var rows = (ValueRow<ValueRow<int>>[])Check.That(() => reader.Read<ValueRow<ValueRow<int>>>(source).ToArray() != null);
			Check.That(
				() => rows.Length == 1,
				() => rows[0].Value.Value == expected.Value.Value);
		}

		public void supports_deeply_nested_fields() {
			var source = new SimpleDataReader("Value.Value.Value");
			var expected = new ValueRow<ValueRow<ValueRow<int>>> { Value = new ValueRow<ValueRow<int>> { Value = new ValueRow<int> { Value = 42 } } };
			source.Add(expected.Value.Value.Value);
			var reader = new ObjectReader();
			var rows = (ValueRow<ValueRow<ValueRow<int>>>[])Check.That(() => reader.Read<ValueRow<ValueRow<ValueRow<int>>>>(source).ToArray() != null);
			Check.That(
				() => rows.Length == 1,
				() => rows[0].Value.Value.Value == expected.Value.Value.Value);
		}

		public void can_read_nullable_field() {
			var source = new SimpleDataReader("Value");
			source.Add(3.14f);
			source.Add(new object[] { null });
			var reader = new ObjectReader();
			Check.That(
				() => reader.Read<ValueRow<float?>>(source).First().Value == 3.14f,
				() => reader.Read<ValueRow<float?>>(source).Last().Value == null);
		}

		struct StructRow<T> { public T Value; }

		public void can_read_structs() {
			var source = new SimpleDataReader("Value");
			var expected = new StructRow<float> { Value = 3.14f };
			source.Add(expected.Value);
			var reader = new ObjectReader();
			var rows = (StructRow<float>[])Check.That(() => reader.Read<StructRow<float>>(source).ToArray() != null);
			Check.That(
				() => rows.Length == 1,
				() => rows[0].Value == expected.Value);
		}

		class MyThing
		{
			public MyThing(int value) { this.Value = value; }
			public int Value { get; }
		}

		public void can_use_parameterized_ctor() {
			var source = new SimpleDataReader("value");
			var expected = new MyThing(42);
			source.Add(expected.Value);
			var reader = new ObjectReader();
			var rows = (MyThing[])Check.That(() => reader.Read<MyThing>(source).ToArray() != null);
			Check.That(
				() => rows.Length == 1,
				() => rows[0].Value == expected.Value);
		}
	}
}