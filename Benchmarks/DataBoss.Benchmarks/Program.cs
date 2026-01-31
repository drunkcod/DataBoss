using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Runtime.CompilerServices;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Running;
using DataBoss.Data;

namespace DataBoss.Benchmarks
{
    [MemoryDiagnoser]
    public class RecordReaderBenchmark
    {
        private const int RowCount = 10000;
        private const int ColumnCount = 20;

        public class FastDataReader : DbDataReader
        {
            private int current = -1;
            private readonly int count;
            public FastDataReader(int count) => this.count = count;
            public override bool Read() => ++current < count;
            public override int FieldCount => ColumnCount;
            public override int GetInt32(int i) => current;
            public override object GetValue(int i) => current;
            public override bool IsDBNull(int i) => false;
            
            public override T GetFieldValue<T>(int i) {
                if (typeof(T) == typeof(int)) return Unsafe.As<int, T>(ref current);
                return base.GetFieldValue<T>(i);
            }

            public override DataTable GetSchemaTable() {
                var schema = new DataReaderSchemaTable();
                for(int i = 0; i < ColumnCount; i++)
                    schema.Add(((char)('A' + i)).ToString(), i, typeof(int), false);
                return schema.ToDataTable();
            }

            public override object this[int i] => GetValue(i);
            public override object this[string name] => GetValue(0);
            public override int Depth => 0;
            public override bool IsClosed => false;
            public override int RecordsAffected => -1;
            public override bool HasRows => true;
            public override void Close() { }
            public override string GetDataTypeName(int i) => "int";
            public override Type GetFieldType(int i) => typeof(int);
            public override string GetName(int i) => "Column" + i;
            public override int GetOrdinal(string name) => 0;
            public override int GetValues(object[] values) => 0;
            public override bool NextResult() => false;
            public override System.Collections.IEnumerator GetEnumerator() => throw new NotImplementedException();
            public override bool GetBoolean(int i) => false;
            public override byte GetByte(int i) => 0;
            public override long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length) => 0;
            public override char GetChar(int i) => ' ';
            public override long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length) => 0;
            public override Guid GetGuid(int i) => Guid.Empty;
            public override short GetInt16(int i) => 0;
            public override long GetInt64(int i) => 0;
            public override float GetFloat(int i) => 0;
            public override double GetDouble(int i) => 0;
            public override string GetString(int i) => "";
            public override decimal GetDecimal(int i) => 0;
            public override DateTime GetDateTime(int i) => DateTime.MinValue;
        }

        [Benchmark(Baseline = true)]
        public void ObjectDataRecordReader_Benchmark()
        {
            using (var reader = new FastDataReader(RowCount))
            {
                var recordReader = new ObjectDataRecordReader(reader);
                while (recordReader.Read())
                {
                    using var record = recordReader.GetRecord();
                    Consume(record);
                }
            }
        }

        [Benchmark]
        public void TupleRecordReader_Benchmark()
        {
            using (var reader = new FastDataReader(RowCount))
            {
                var recordReader = new TupleRecordReader(reader);
                while (recordReader.Read())
                {
                    using var record = recordReader.GetRecord();
                    Consume(record);
                }
            }
        }

        private void Consume(IDataRecord record)
        {
            var a = record.GetInt32(0);
            var b = record.GetInt32(1);
        }
    }

    class Program
    {
        static void Main(string[] args)
        {
            BenchmarkRunner.Run<RecordReaderBenchmark>();
        }
    }
}