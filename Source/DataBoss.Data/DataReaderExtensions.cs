using System;
using System.Data;
using System.Runtime.CompilerServices;

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
				DataTypeName: sourceSchema.Columns["DataTypeName"],
				AllowDBNull: sourceSchema.Columns["AllowDBNull"]);

			for(var i = 0; i != sourceSchema.Rows.Count; ++i) {
				var item = sourceSchema.Rows[i];
				var row = (
					 Name: (string)item[columns.ColumnName],
					 Ordinal: (int)item[columns.ColumnOrdinal],
					 DataType: (Type)item[columns.DataType],
					 DataTypeName: DefaultIfDBNull<string>(item[columns.DataTypeName]),
					 AllowDBNull: (bool)item[columns.AllowDBNull],
					 Size: (int)item[columns.ColumnSize]);
				schema.Add(row.Name, row.Ordinal, row.DataType, row.AllowDBNull, row.Size, row.DataTypeName);
			}
			return schema;
		}

		static T DefaultIfDBNull<T>(object obj) {
			if (obj == DBNull.Value)
				return default;
			return (T)obj;
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public static T GetFieldValue<T>(this IDataRecord record, int i) {
			if (typeof(T) == typeof(short))
				return (T)(object)record.GetInt16(i);
			else if (typeof(T) == typeof(int))
				return (T)(object)record.GetInt32(i);
			else if (typeof(T) == typeof(long))
				return (T)(object)record.GetInt64(i);
			else if (typeof(T) == typeof(float))
				return (T)(object)record.GetFloat(i);
			else if (typeof(T) == typeof(double))
				return (T)(object)record.GetDouble(i);
			else if (typeof(T) == typeof(decimal))
				return (T)(object)record.GetDecimal(i);
			else if (typeof(T) == typeof(bool))
				return (T)(object)record.GetBoolean(i);
			else if (typeof(T) == typeof(byte))
				return (T)(object)record.GetByte(i);
			else if (typeof(T) == typeof(char))
				return (T)(object)record.GetChar(i);
			else if (typeof(T) == typeof(DateTime))
				return (T)(object)record.GetDateTime(i);
			else if (typeof(T) == typeof(Guid))
				return (T)(object)record.GetGuid(i);
			else if (typeof(T) == typeof(string))
				return (T)(object)record.GetString(i);
			else
				return (T)record.GetValue(i);
		}
	}
}
