using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;

namespace DataBoss.Data.DataFrames
{
	public class DataFrame {
		delegate void FieldReader(IDataReader r, int i);

		IDataFrameColumn[] columns;
		readonly Dictionary<string, int> columnNames = new();

		public static DataFrame Create<T>(IEnumerable<T> data) =>
			Create(SequenceDataReader.Create(data));

		public static DataFrame Create(IDataReader reader) {
			var df = new DataFrame();
			var schema = reader.GetSchemaTable();
			var name = schema.Columns[DataReaderSchemaColumns.ColumnName.Name];
			var type = schema.Columns[DataReaderSchemaColumns.DataType.Name];
			var allowNull = schema.Columns[DataReaderSchemaColumns.AllowDBNull.Name];
			var ordinal = schema.Columns[DataReaderSchemaColumns.ColumnOrdinal.Name];

			var readField = GetMethod(nameof(ReadField));
			var readNullableField = GetMethod(nameof(ReadNullableField));

			var fieldReaders = new FieldReader[reader.FieldCount];
			var columns = new IDataFrameColumnBuilder[reader.FieldCount];
			var fieldType = new Type[reader.FieldCount];
			for (var i = 0; i != reader.FieldCount; ++i) {
				var r = schema.Rows[i];
				var o = (int)r[ordinal];
				var isNullable = (bool)r[allowNull];
				var columnType = fieldType[o] = (Type)r[type];
				var readMethod = readField;

				if (isNullable && columnType.IsValueType) {
					readMethod = readNullableField;
					columnType = typeof(Nullable<>).MakeGenericType(columnType);
				}

				var columnName = (string)r[name];
				columns[o] = CreateColumn(columnType, columnName);
				df.columnNames.Add(columnName, o);
				fieldReaders[o] = CreateFieldReader(
					readMethod.MakeGenericMethod(fieldType[o]),
					columns[o]);
			}

			while (reader.Read())
				for (var i = 0; i != columns.Length; ++i)
					fieldReaders[i](reader, i);

			df.columns = new IDataFrameColumn[columns.Length];
			for (var i = 0; i != columns.Length; ++i) 
				df.columns[i] = columns[i].Build();

			return df;
		}

		static MethodInfo GetMethod(string name) =>
			typeof(DataFrame).GetMethod(name, BindingFlags.Static | BindingFlags.NonPublic);

		static FieldReader CreateFieldReader(MethodInfo method, object target) =>
			(FieldReader)Delegate.CreateDelegate(typeof(FieldReader), target, method);

		static void ReadField<T>(DataFrameColumnBuilder<T> xs, IDataRecord r, int i) =>
			xs.Add(r.GetFieldValue<T>(i));

		static void ReadNullableField<T>(DataFrameColumnBuilder<T?> xs, IDataRecord r, int i) where T : struct =>
			xs.Add(r.IsDBNull(i) ? null : r.GetFieldValue<T>(i));

		public IReadOnlyList<IDataFrameColumn> Columns => columns;
		public int Count => Columns.Count > 0 ? Columns[0].Count : 0;

		static IDataFrameColumnBuilder CreateColumn(Type columnType, string name) =>
			(IDataFrameColumnBuilder)Activator.CreateInstance(typeof(DataFrameColumnBuilder<>).MakeGenericType(columnType), name);

		public IDataFrameColumn this[string columnName] => columns[columnNames[columnName]];

		public DataFrame this[DataFrameColumn<bool> bs] {
			get {
				var df = new DataFrame();
				df.columns = Array.ConvertAll(columns, x => x.Filter(bs));
				return df;
			}
		}

		interface IDataFrameColumnBuilder
		{
			IDataFrameColumn Build();
		}

		class DataFrameColumnBuilder<T> : IDataFrameColumnBuilder
		{
			readonly List<T> items = new();
			readonly string name;

			public DataFrameColumnBuilder(string name) {
				this.name = name;
			}

			public void Add(T item) => items.Add(item);

			public IDataFrameColumn Build() => new DataFrameColumn<T>(items, name);
		}

		public DataFrameColumn<T> GetColumn<T>(string name) =>
			(DataFrameColumn<T>)columns[columnNames[name]];

	}

	public interface IDataFrameColumn : IEnumerable
	{
		int Count { get; }
		string Name { get; }

		IDataFrameColumn Filter(IEnumerable<bool> include);
	}

	public class DataFrameColumn<T> : IDataFrameColumn, IEnumerable<T>
	{
		readonly T[] items;
		readonly string name;

		public DataFrameColumn(IEnumerable<T> items, string name = null) {
			this.items = items.ToArray();
			this.name = name ?? string.Empty;
		}

		public int Count => items.Length;
		public string Name => name;

		public static DataFrameColumn<bool> operator >(DataFrameColumn<T> xs, T value) {
			var c = Comparer<T>.Default;
			return new DataFrameColumn<bool>(Array.ConvertAll(xs.items, x => c.Compare(x, value) > 0));
		}

		public static DataFrameColumn<bool> operator <(DataFrameColumn<T> xs, T value) {
			var c = Comparer<T>.Default;
			return new DataFrameColumn<bool>(Array.ConvertAll(xs.items, x => c.Compare(x, value) < 0));
		}

		public IDataFrameColumn Filter(IEnumerable<bool> include) =>
			new DataFrameColumn<T>(items
				.Zip(include, (x, include) => (x, include))
				.Where(x => x.include)
				.Select(x => x.x));

		public IEnumerator<T> GetEnumerator() => ((IEnumerable<T>)items).GetEnumerator();

		IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
	}
}
