using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;

namespace DataBoss.Data
{
	public class DataSeriesReader
	{
		class EnumerableDataSeries<T> : DataSeries<T>, IEnumerable<T>
		{
			public EnumerableDataSeries(string name, bool allowNulls) : base(name, allowNulls) { }

			IEnumerator<T> IEnumerable<T>.GetEnumerator() => GetEnumerator();
			IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
			IEnumerator<T> GetEnumerator() {
				for (var i = 0; i != Count; ++i)
					yield return this[i];
			}
		}

		class NullableDataSeries<T> : DataSeries<T>, IEnumerable<T?> where T : struct
		{
			public NullableDataSeries(string name) : base(name, true) { }

			IEnumerator<T?> IEnumerable<T?>.GetEnumerator() => GetEnumerator();
			IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
			IEnumerator<T?> GetEnumerator() {
				for (var i = 0; i != Count; ++i)
					if (IsNull(i))
						yield return null;
					else yield return this[i];
			}
		}

		static DataSeries CreateDataSeries(string name, Type type, bool allowNulls) =>
			(type.IsValueType && allowNulls)
			? Lambdas.CreateDelegate<Func<string, DataSeries>>(CreateNullableSeriesMethod.MakeGenericMethod(type))(name)
			: Lambdas.CreateDelegate<Func<string, bool, DataSeries>>(CreateDataSeriesMethod.MakeGenericMethod(type))(name, allowNulls);

		static readonly MethodInfo CreateDataSeriesMethod = GetGenericMethod(nameof(CreateDataSeries));
		static readonly MethodInfo CreateNullableSeriesMethod = GetGenericMethod(nameof(CreateNullableSeries));

		static MethodInfo GetGenericMethod(string name) =>
			typeof(DataSeriesReader)
			.GetMethods(BindingFlags.Static | BindingFlags.NonPublic | BindingFlags.DeclaredOnly)
			.Single(x => x.IsGenericMethod && x.Name == name);

		static DataSeries CreateDataSeries<T>(string name, bool allowNulls) =>
			new EnumerableDataSeries<T>(name, allowNulls);

		static DataSeries CreateNullableSeries<T>(string name) where T : struct =>
			new NullableDataSeries<T>(name);

		readonly IDataReader reader;
		readonly DataReaderSchemaTable schema;
		readonly List<DataSeries> series = new();
		readonly List<int> ordinals = new();

		public DataSeriesReader(IDataReader reader) {
			this.reader = reader;
			this.schema = reader.GetDataReaderSchemaTable();
		}

		public static IReadOnlyList<DataSeries> ReadAll(IDataReader reader) {
			var xs = new DataSeriesReader(reader);
			foreach (var column in xs.schema)
				xs.Add(column);
			return xs.Read();
		}

		public void Add(string name) =>
			Add(schema.Single(x => x.ColumnName == name));

		void Add(DataReaderSchemaRow columnSchema) {
			var data = CreateDataSeries(columnSchema.ColumnName, columnSchema.ColumnType, columnSchema.AllowDBNull);
			Add(data, columnSchema.Ordinal);
		}

		void Add(DataSeries item, int ordinal) {
			series.Add(item);
			ordinals.Add(ordinal);
		}

		public IReadOnlyList<DataSeries> Read() {
			while (reader.Read())
				foreach (var (item, ordinal) in series.Zip(ordinals, (s, o) => (s, o)))
					item.ReadItem(reader, ordinal);

			return series;
		}
	}

}
