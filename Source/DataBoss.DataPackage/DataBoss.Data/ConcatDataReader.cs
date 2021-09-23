using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;

namespace DataBoss.Data
{
	public class ConcatDataReader : DataReaderDecoratorBase
	{
		readonly DataReaderSchemaTable schema;
		readonly DbDataReader[] readers;
		int next;

		ConcatDataReader(DataReaderSchemaTable schema, DbDataReader[] readers) : base(GetFirst(readers)) {
			this.readers = readers;
			this.next = 1;
			this.schema = schema;
		}

		static DbDataReader GetFirst(DbDataReader[] readers) {
			if (readers.Length == 0)
				throw new InvalidOperationException("Can't concatenate zero readers.");
			return readers[0];
		}

		public static ConcatDataReader Create(IDataReader[] readers) =>
			Create(Array.ConvertAll(readers, DataReaderExtensions.AsDbDataReader));

		public static ConcatDataReader Create(DbDataReader[] readers) {
			if (readers.Length == 0)
				throw new InvalidOperationException();

			var resultSchema = readers[0].GetDataReaderSchemaTable();
			for(var i = 1; i != readers.Length; ++i) {
				var readerSchema = readers[i].GetDataReaderSchemaTable();
				if (readerSchema.Count < resultSchema.Count)
					throw new InvalidOperationException($"Too few Columns available for reader {i}.");
				for(var o = 0; o != resultSchema.Count; ++o) {
					var expected = resultSchema[o];
					var actual = readerSchema[o];

					if (actual.ColumnName != expected.ColumnName)
						ColumnMismatch(o, nameof(DataReaderSchemaRow.ColumnName), expected.ColumnName, actual.ColumnName);
					
					if (actual.DataType != expected.DataType)
						ColumnMismatch(o, nameof(DataReaderSchemaRow.DataType), expected.DataType, actual.DataType);

					if(actual.AllowDBNull && !expected.AllowDBNull)
						ColumnMismatch(o, nameof(DataReaderSchemaRow.AllowDBNull), expected.AllowDBNull, actual.AllowDBNull);

					if (actual.ProviderSpecificDataType != expected.ProviderSpecificDataType)
						resultSchema[o].ProviderSpecificDataType = null;
				}
			}

			if(readers.Any(x => x is ConcatDataReader)) {
				var allReaders = new List<DbDataReader>();
				foreach (var item in readers)
					if (item is ConcatDataReader cat)
						allReaders.AddRange(cat.readers);
					else allReaders.Add(item);
				readers = allReaders.ToArray();
			}

			return new ConcatDataReader(resultSchema, readers);
		}

		static void ColumnMismatch(int ordinal, string attributeName, object expected, object actual) =>
			throw new InvalidOperationException($"{attributeName} mismatch at column {ordinal}. Expected {expected} but got {actual}.");

		public int ReaderCount => readers.Length;

		public override int RecordsAffected => 0;

		public override void Close() {
			do {
				Inner.Close();
			} while (NextReader());
		}

		protected override void Dispose(bool disposing) {
			if(!disposing)
				return;

			while (NextReader())
				;
			Inner.Dispose();
		}

		bool NextReader() {
			if(next != readers.Length) {
				Inner.Dispose();
				Inner = readers[next++];
				return true;
			}
			return false;
		}

		public override string GetDataTypeName(int i) => schema[i].DataTypeName;
		public override Type GetFieldType(int i) => schema[i].DataType;
		public override string GetName(int i) => schema[i].ColumnName;
		public override int GetOrdinal(string name) => schema.GetOrdinal(name);
		public override DataTable GetSchemaTable() => schema.ToDataTable();
		public override Type GetProviderSpecificFieldType(int i) => schema[i].ProviderSpecificDataType;

		public override bool NextResult() => false;

		public override bool Read() {
			while (base.Read() == false)
				if (!NextReader())
					return false;
			return true;
		}
	}
}
