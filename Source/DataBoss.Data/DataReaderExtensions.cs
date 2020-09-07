using System;
using System.Data;

namespace DataBoss.Data
{
	public static class DataReaderExtensions
	{
		public static void ForAll<TReader, T>(this TReader self, Action<T> action) where TReader : IDataReader => CallForAll(self, action);
		public static void ForAll<TReader, T1, T2>(this TReader self, Action<T1, T2> action) where TReader : IDataReader => CallForAll(self, action);
		public static void ForAll<TReader, T1, T2, T3>(this TReader self, Action<T1, T2, T3> action) where TReader : IDataReader => CallForAll(self, action);
		public static void ForAll<TReader, T1, T2, T3, T4>(this TReader self, Action<T1, T2, T3, T4> action) where TReader : IDataReader => CallForAll(self, action);
		public static void ForAll<TReader, T1, T2, T3, T4, T5>(this TReader self, Action<T1, T2, T3, T4, T5> action) where TReader : IDataReader => CallForAll(self, action);
		public static void ForAll<TReader, T1, T2, T3, T4, T5, T6>(this TReader self, Action<T1, T2, T3, T4, T5, T6> action) where TReader : IDataReader => CallForAll(self, action);
		public static void ForAll<TReader, T1, T2, T3, T4, T5, T6, T7>(this TReader self, Action<T1, T2, T3, T4, T5, T6, T7> action) where TReader : IDataReader => CallForAll(self, action);

		static void CallForAll<TReader, TTarget>(TReader self, TTarget target) where TReader : IDataReader where TTarget : Delegate {
			var converter = ConverterFactory.Default.GetTrampoline(self, target);
			var jump = (Action<TReader, TTarget>)converter.Compile();
			while (self.Read())
				jump(self, target);
		}

		public static DataReaderSchemaTable GetDataReaderSchemaTable(this IDataReader self) {
			var schema = new DataReaderSchemaTable();
			var sourceSchema = self.GetSchemaTable();
			var columns = (
				ColumnName: sourceSchema.Columns["ColumnName"],
				ColumnOrdinal: sourceSchema.Columns["ColumnOrdinal"],
				ColumnSize: sourceSchema.Columns["ColumnSize"],
				DataType: sourceSchema.Columns["DataType"],
				AllowDBNull: sourceSchema.Columns["AllowDBNull"]);

			for(var i = 0; i != sourceSchema.Rows.Count; ++i) {
				var item = sourceSchema.Rows[i];
				var row = (
					 Name: (string)item[columns.ColumnName],
					 Ordinal: (int)item[columns.ColumnOrdinal],
					 DataType: (Type)item[columns.DataType],
					 AllowDBNull: (bool)item[columns.AllowDBNull],
					 Size: (int)item[columns.ColumnSize]);
				schema.Add(row.Name, row.Ordinal, row.DataType, row.AllowDBNull, row.Size);
			}
			return schema;
		}
	}
}
