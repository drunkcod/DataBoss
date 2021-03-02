using System; 
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace DataBoss.Data
{
	using NewtonsoftJsonConverter = Newtonsoft.Json.JsonConverter;
	using NewtonsoftJsonConverterAttribute = Newtonsoft.Json.JsonConverterAttribute;
	using NewtonsoftJsonReader = Newtonsoft.Json.JsonReader;
	using NewtonsoftJsonSerializer = Newtonsoft.Json.JsonSerializer;
	using NewtonsoftJsonWriter = Newtonsoft.Json.JsonWriter;

	public static class DataReaderJsonExtensions
	{
		class NewtonsoftRecordWriter
		{
			delegate void FieldWriter(string name, DataSeries column);

			readonly NewtonsoftJsonWriter json;
			readonly NewtonsoftJsonSerializer serializer;
			readonly IReadOnlyList<DataSeries> data;
			readonly string[] fieldName;
			readonly FieldWriter[] fieldWriter;
			int row = -1;

			public NewtonsoftRecordWriter(NewtonsoftJsonWriter json, NewtonsoftJsonSerializer serializer, IReadOnlyList<DataSeries> data) {
				this.json = json;
				this.serializer = serializer;
				this.data = data;
				this.fieldName = new string[data.Count];
				this.fieldWriter = new FieldWriter[data.Count];
				
				var writeField = typeof(NewtonsoftRecordWriter).GetMethod(nameof(WriteField), BindingFlags.Instance | BindingFlags.NonPublic);
				var namingPolicy = GetNamingPolicy(serializer);
				for (var i = 0; i != fieldWriter.Length; ++i) {
					fieldName[i] = namingPolicy(data[i].Name);
					fieldWriter[i] = Lambdas.CreateDelegate<FieldWriter>(this, writeField.MakeGenericMethod(data[i].Type));
				}
			}

			public void WriteCurrent() {
				json.WriteStartObject();
				for (var i = 0; i != fieldWriter.Length; ++i)
					fieldWriter[i](fieldName[i], data[i]);
				json.WriteEndObject();
			}

			public bool MoveNext() =>
				++row < data[0].Count;

			void WriteField<T>(string name, DataSeries column) {
				json.WritePropertyName(name);
				if (column.IsNull(row))
					json.WriteNull();
				else WriteValue<T>(column);
			}

			void WriteValue<T>(DataSeries column) {
				if (typeof(T) == typeof(short))
					json.WriteValue(column.GetValue<short>(row));
				else if (typeof(T) == typeof(int))
					json.WriteValue(column.GetValue<int>(row));
				else if (typeof(T) == typeof(long))
					json.WriteValue(column.GetValue<long>(row));
				else if (typeof(T) == typeof(float))
					json.WriteValue(column.GetValue<float>(row));
				else if (typeof(T) == typeof(double))
					json.WriteValue(column.GetValue<double>(row));
				else if (typeof(T) == typeof(decimal))
					json.WriteValue(column.GetValue<decimal>(row));
				else if (typeof(T) == typeof(bool))
					json.WriteValue(column.GetValue<bool>(row));
				else if (typeof(T) == typeof(string))
					json.WriteValue(column.GetValue<string>(row));
				else if (typeof(T) == typeof(DateTime))
					json.WriteValue(column.GetValue<DateTime>(row));
				else serializer.Serialize(json, column.GetValue<object>(row), typeof(T));
			}
		}

		class Utf8RecordWriter
		{
			delegate void FieldWriter(in JsonEncodedText name, DataSeries column);

			readonly Utf8JsonWriter json;
			readonly IReadOnlyList<DataSeries> data;
			readonly JsonEncodedText[] fieldName;
			readonly FieldWriter[] fieldWriter;
			readonly JsonSerializerOptions options;
			int row = -1;

			public Utf8RecordWriter(Utf8JsonWriter json, JsonSerializerOptions options, IReadOnlyList<DataSeries> data) {
				this.json = json;
				this.options = options;
				this.data = data;
				this.fieldName = new JsonEncodedText[data.Count];
				this.fieldWriter = new FieldWriter[data.Count];

				var writeField = GetType().GetMethod(nameof(WriteField), BindingFlags.Instance | BindingFlags.NonPublic);
				var namingPolicy = GetNamingPolicy(options);
				for (var i = 0; i != fieldWriter.Length; ++i) {
					fieldName[i] = JsonEncodedText.Encode(namingPolicy(data[i].Name));
					fieldWriter[i] = Lambdas.CreateDelegate<FieldWriter>(this, writeField.MakeGenericMethod(data[i].Type));
				}
			}

			public bool MoveNext() =>
				++row < data[0].Count;

			public void WriteCurrent() {
				json.WriteStartObject();
				for (var i = 0; i != fieldWriter.Length; ++i)
					fieldWriter[i](fieldName[i], data[i]);
				json.WriteEndObject();
			}

			void WriteField<T>(in JsonEncodedText name, DataSeries column) {
				if (column.IsNull(row))
					json.WriteNull(name);
				else
					WriteValue<T>(name, column);
			}

			void WriteValue<T>(in JsonEncodedText name, DataSeries column) {
				if (typeof(T) == typeof(short))
					json.WriteNumber(name, column.GetValue<short>(row));
				else if (typeof(T) == typeof(int))
					json.WriteNumber(name, column.GetValue<int>(row));
				else if (typeof(T) == typeof(long))
					json.WriteNumber(name, column.GetValue<long>(row));
				else if (typeof(T) == typeof(float))
					json.WriteNumber(name, column.GetValue<float>(row));
				else if (typeof(T) == typeof(double))
					json.WriteNumber(name, column.GetValue<double>(row));
				else if (typeof(T) == typeof(decimal))
					json.WriteNumber(name, column.GetValue<decimal>(row));
				else if (typeof(T) == typeof(bool))
					json.WriteBoolean(name, column.GetValue<bool>(row));
				else if (typeof(T) == typeof(string))
					json.WriteString(name, column.GetValue<string>(row));
				else if (typeof(T) == typeof(Guid))
					json.WriteString(name, column.GetValue<Guid>(row));
				else if (typeof(T) == typeof(DateTime))
					json.WriteString(name, column.GetValue<DateTime>(row));
				else {
					json.WritePropertyName(name);
					JsonSerializer.Serialize(json, column.GetValue<object>(row), typeof(T), options);
				}
			}
		}

		[NewtonsoftJsonConverter(typeof(NewtonsoftDataReaderJsonConverter))]
		public interface IDataReaderJsonObject
		{
			void Write(NewtonsoftJsonWriter json, NewtonsoftJsonSerializer serializer);
			void Write(Utf8JsonWriter json, JsonSerializerOptions options);
		}

		[JsonConverter(typeof(DataReaderJsonConverter<DataRecordJsonObject>))]
		public class DataRecordJsonObject : IDataReaderJsonObject
		{
			static readonly HashSet<Type> seenTypes = new();
			static Func<IDataRecord, int, IDataReaderJsonObject> GetRecordValueDispatch = delegate { return null; };

			class RecordValue<T> : IDataReaderJsonObject
			{
				readonly T value;

				public RecordValue(T value) { this.value = value; }
				
				public void Write(NewtonsoftJsonWriter json, NewtonsoftJsonSerializer serializer) {
					if (typeof(T) == typeof(bool)) json.WriteValue((bool)(object)value);
					else if (typeof(T) == typeof(byte)) json.WriteValue((byte)(object)value);
					else if (typeof(T) == typeof(short)) json.WriteValue((short)(object)value);
					else if (typeof(T) == typeof(int)) json.WriteValue((int)(object)value);
					else if (typeof(T) == typeof(long)) json.WriteValue((long)(object)value);
					else if (typeof(T) == typeof(float)) json.WriteValue((float)(object)value);
					else if (typeof(T) == typeof(double)) json.WriteValue((double)(object)value);
					else if (typeof(T) == typeof(string)) json.WriteValue((string)(object)value);
					else if (typeof(T) == typeof(DateTime)) json.WriteValue((DateTime)(object)value);
					else if (typeof(T) == typeof(Guid)) json.WriteValue((Guid)(object)value);
					else serializer.Serialize(json, value);
				}

				public void Write(Utf8JsonWriter json, JsonSerializerOptions options) {
					if (typeof(T) == typeof(bool)) json.WriteBooleanValue((bool)(object)value);
					else if (typeof(T) == typeof(byte)) json.WriteNumberValue((byte)(object)value);
					else if (typeof(T) == typeof(short)) json.WriteNumberValue((short)(object)value);
					else if (typeof(T) == typeof(int)) json.WriteNumberValue((int)(object)value);
					else if (typeof(T) == typeof(long)) json.WriteNumberValue((long)(object)value);
					else if (typeof(T) == typeof(float)) json.WriteNumberValue((float)(object)value);
					else if (typeof(T) == typeof(double)) json.WriteNumberValue((double)(object)value);
					else if (typeof(T) == typeof(string)) json.WriteStringValue((string)(object)value);
					else if (typeof(T) == typeof(DateTime)) json.WriteStringValue((DateTime)(object)value);
					else if (typeof(T) == typeof(Guid)) json.WriteStringValue((Guid)(object)value);
					else JsonSerializer.Serialize(json, value, typeof(T), options);
				}
			}

			class NullValue : IDataReaderJsonObject
			{
				public void Write(NewtonsoftJsonWriter json, NewtonsoftJsonSerializer serializer) =>
					json.WriteNull();

				public void Write(Utf8JsonWriter json, JsonSerializerOptions options) =>
					json.WriteNullValue();

				NullValue() { }

				public static NullValue Instance = new NullValue();
			}

			readonly List<(string Name, IDataReaderJsonObject Value)> values = new();

			public static DataRecordJsonObject Read(IDataRecord record) {
				var jrecord = new DataRecordJsonObject();
				for (var i = 0; i != record.FieldCount; ++i)
					jrecord.values.Add((record.GetName(i), GetRecordValue(record, i)));
				return jrecord;
			}

			static IDataReaderJsonObject GetRecordValue(IDataRecord record, int i) {
				if (record.IsDBNull(i))
					return NullValue.Instance;

				var value = GetRecordValueDispatch(record, i);
				if (value != null)
					return value;
				lock(seenTypes)
					if(seenTypes.Add(record.GetFieldType(i))) {
						var m = typeof(DataRecordJsonObject).GetMethod(nameof(GetRecordValueT), BindingFlags.Static | BindingFlags.NonPublic);
						var p0 = Expression.Parameter(typeof(IDataRecord));
						var p1 = Expression.Parameter(typeof(int));

						var dispatchList = 
							seenTypes.Select(x => Expression.SwitchCase(
								Expression.Call(m.MakeGenericMethod(x), p0, p1),
								Expression.Constant(x)))
							.ToArray();

						GetRecordValueDispatch = Expression.Lambda<Func<IDataRecord, int, IDataReaderJsonObject>>(
							Expression.Switch(
								Expression.Call(p0, p0.Type.GetMethod(nameof(IDataRecord.GetFieldType)), p1), 
								Expression.Constant(null, typeof(IDataReaderJsonObject)),
								dispatchList),
								p0, p1).Compile();
					}
				return GetRecordValueDispatch(record, i);
			}

			static IDataReaderJsonObject GetRecordValueT<T>(IDataRecord record, int i) =>
				new RecordValue<T>(record.GetFieldValue<T>(i));

			public void Write(NewtonsoftJsonWriter json, NewtonsoftJsonSerializer serializer) {
				json.WriteStartObject();
				var namingPolicy = GetNamingPolicy(serializer);
				foreach(var item in values) {
					json.WritePropertyName(namingPolicy(item.Name));
					item.Value.Write(json, serializer);
				}
				json.WriteEndObject();
			}

			public void Write(Utf8JsonWriter json, JsonSerializerOptions options) {
				json.WriteStartObject();
				var namingPolicy = GetNamingPolicy(options);
				foreach (var item in values) {
					json.WritePropertyName(namingPolicy(item.Name));
					item.Value.Write(json, options);
				}
				json.WriteEndObject();
			}
		}

		[JsonConverter(typeof(DataReaderJsonConverter<DataReaderJsonArray>))]
		public class DataReaderJsonArray : IDataReaderJsonObject
		{
			readonly IReadOnlyList<DataSeries> series;

			DataReaderJsonArray(IReadOnlyList<DataSeries> series) {
				this.series = series;
			}

			public static DataReaderJsonArray Read(IDataReader reader) {
				var series = DataSeriesReader.ReadAll(reader);
				return new DataReaderJsonArray(series);
			}

			public void Write(NewtonsoftJsonWriter json, NewtonsoftJsonSerializer serializer) {
				var records = new NewtonsoftRecordWriter(json, serializer, series);
				json.WriteStartArray();
				while (records.MoveNext())
					records.WriteCurrent();
				json.WriteEndArray();
			}

			public void Write(Utf8JsonWriter json, JsonSerializerOptions options = null) {
				var records = new Utf8RecordWriter(json, options, series);
				json.WriteStartArray();
				while (records.MoveNext())
					records.WriteCurrent();
				json.WriteEndArray();
			}
		}

		[JsonConverter(typeof(DataReaderJsonConverter<DataReaderJsonColumns>))]
		public class DataReaderJsonColumns : IDataReaderJsonObject
		{
			readonly IReadOnlyList<DataSeries> data;

			DataReaderJsonColumns(IReadOnlyList<DataSeries> data) {
				this.data = data;
			}

			public static DataReaderJsonColumns Read(IDataReader reader) =>
				new DataReaderJsonColumns(DataSeriesReader.ReadAll(reader));

			public void Write(NewtonsoftJsonWriter json, NewtonsoftJsonSerializer serializer) {
				json.WriteStartObject();
				var namingPolicy = GetNamingPolicy(serializer);
				foreach(var c in data) {
					json.WritePropertyName(namingPolicy(c.Name));
					serializer.Serialize(json, c);
				}
				json.WriteEndObject();
			}

			public void Write(Utf8JsonWriter json, JsonSerializerOptions options = null) {
				json.WriteStartObject();
				var namingPolicy = GetNamingPolicy(options);
				foreach(var c in data) {
					json.WritePropertyName(namingPolicy(c.Name));
					JsonSerializer.Serialize(json, c, c.GetType(), options);
				}
				json.WriteEndObject();
			}
		}

		static Func<string, string> GetNamingPolicy(NewtonsoftJsonSerializer serializer) {
			var resolver = serializer.ContractResolver as Newtonsoft.Json.Serialization.DefaultContractResolver;
			return resolver == null ? KeepNameAsIs : resolver.GetResolvedPropertyName;
		}

		static Func<string, string> GetNamingPolicy(JsonSerializerOptions options) {
			var namingPolicy = options?.PropertyNamingPolicy;
			return namingPolicy == null ? KeepNameAsIs : namingPolicy.ConvertName;
		}

		static string KeepNameAsIs(string input) => input;

		public class NewtonsoftDataReaderJsonConverter : NewtonsoftJsonConverter
		{
			public override bool CanConvert(Type objectType) => typeof(IDataReaderJsonObject).IsAssignableFrom(objectType);

			public override object ReadJson(NewtonsoftJsonReader reader, Type objectType, object existingValue, NewtonsoftJsonSerializer serializer) {
				throw new NotImplementedException();
			}

			public override void WriteJson(NewtonsoftJsonWriter writer, object value, NewtonsoftJsonSerializer serializer) =>
				((IDataReaderJsonObject)value).Write(writer, serializer);
		}

		public class DataReaderJsonConverter<T> : JsonConverter<T> where T : IDataReaderJsonObject
		{
			public override T Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options) {
				throw new NotImplementedException();
			}

			public override void Write(Utf8JsonWriter writer, T value, JsonSerializerOptions options) =>
				value.Write(writer, options);
		}

		public static DataRecordJsonObject ToJsonObject(this IDataRecord r) => DataRecordJsonObject.Read(r); 
		public static DataReaderJsonArray ToJsonArray(this IDataReader r) => DataReaderJsonArray.Read(r);
		public static DataReaderJsonColumns ToJsonColumns(this IDataReader r) => DataReaderJsonColumns.Read(r);
		public static string ToJson(this IDataReader r) {
			var bytes = new MemoryStream();
			using var json = new Utf8JsonWriter(bytes);
			r.ToJsonArray().Write(json);
			json.Flush();
			return bytes.TryGetBuffer(out var buffer)
			? Encoding.UTF8.GetString(buffer.Array, buffer.Offset, buffer.Count)
			: Encoding.UTF8.GetString(bytes.ToArray());
		}
	}

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

	public abstract class DataSeries
	{
		readonly BitArray isNull;

		public DataSeries(Type type, bool allowNulls) {
			this.Type = type;
			this.isNull = allowNulls ? new BitArray(0) : null;
		}

		public bool AllowNulls => isNull != null;
		public abstract int Count { get; }
		public abstract string Name { get; }

		internal void ReadItem(IDataRecord record, int ordinal) {
			if (AllowNulls) {

				var isNullItem = record.IsDBNull(ordinal);
				isNull.Length = Count + 1;
				isNull[Count] = isNullItem;

				if (isNullItem) {
					AddDefault();
					return;
				}
			}
			AddItem(record, ordinal);
		}

		protected virtual void AddDefault() { }
		protected virtual void AddItem(IDataRecord record, int ordinal) { }

		public Type Type { get; }
		public bool IsNull(int index) => AllowNulls && isNull[index];
		public abstract T GetValue<T>(int index);
	}

	public class DataSeries<TItem> : DataSeries
	{
		readonly List<TItem> items = new();

		public DataSeries(string name, bool allowNulls) : base(typeof(TItem), allowNulls) {
			this.Name = name;
		}

		public override int Count => items.Count;
		public override string Name { get; }
		public TItem this[int index] => items[index];

		public override T GetValue<T>(int index) =>
			(T)(object)items[index];

		protected override void AddDefault() =>
			items.Add(default);

		protected override void AddItem(IDataRecord record, int ordinal) =>
			items.Add(record.GetFieldValue<TItem>(ordinal));

	}
}
