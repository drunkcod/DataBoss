using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Reflection;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace DataBoss.Data
{
	using NewtonsoftJsonConverter = Newtonsoft.Json.JsonConverter;
	using NewtonsoftJsonConverterAttribute = Newtonsoft.Json.JsonConverterAttribute;
	using NewtonsoftJsonReader = Newtonsoft.Json.JsonReader;
	using NewtonsoftJsonWriter = Newtonsoft.Json.JsonWriter;
	using NewtonsoftJsonSerializer = Newtonsoft.Json.JsonSerializer;

	public static class DataReaderJsonExtensions
	{
		class NewtonsoftRecordWriter : IDisposable
		{
			readonly NewtonsoftJsonWriter json;
			readonly NewtonsoftJsonSerializer serializer;
			readonly IDataReader reader;
			readonly Action<int>[] fieldWriter;

			public NewtonsoftRecordWriter(NewtonsoftJsonWriter json, NewtonsoftJsonSerializer serializer, IDataReader reader) {
				this.json = json;
				this.serializer = serializer;
				this.reader = reader;
				this.fieldWriter = new Action<int>[reader.FieldCount];
				for (var i = 0; i != fieldWriter.Length; ++i)
					fieldWriter[i] = GetFieldWriter(reader.GetFieldType(i));
			}

			public void Dispose() => reader.Close();
			public bool Read() => reader.Read();

			public void WriteRecord() {
				json.WriteStartObject();
				for (var i = 0; i != fieldWriter.Length; ++i)
					fieldWriter[i](i);
				json.WriteEndObject();
			}

			public Action<int> GetFieldWriter(Type t) =>
				Lambdas.CreateDelegate<Action<int>>(this,
					GetType().GetMethod(nameof(WriteField), BindingFlags.Instance | BindingFlags.NonPublic).MakeGenericMethod(t));

			void WriteField<T>(int ordinal) {
				json.WritePropertyName(reader.GetName(ordinal));
				if (reader.IsDBNull(ordinal))
					json.WriteNull();
				else WriteValue<T>(ordinal);
			}

			void WriteValue<T>(int ordinal) {
				if (typeof(T) == typeof(short))
					json.WriteValue(reader.GetInt16(ordinal));
				else if (typeof(T) == typeof(int))
					json.WriteValue(reader.GetInt32(ordinal));
				else if (typeof(T) == typeof(long))
					json.WriteValue(reader.GetInt64(ordinal));
				else if (typeof(T) == typeof(float))
					json.WriteValue(reader.GetFloat(ordinal));
				else if (typeof(T) == typeof(double))
					json.WriteValue(reader.GetDouble(ordinal));
				else if (typeof(T) == typeof(decimal))
					json.WriteValue(reader.GetDecimal(ordinal));
				else if (typeof(T) == typeof(bool))
					json.WriteValue(reader.GetBoolean(ordinal));
				else if (typeof(T) == typeof(string))
					json.WriteValue(reader.GetString(ordinal));
				else if (typeof(T) == typeof(DateTime))
					json.WriteValue(reader.GetDateTime(ordinal));
				else serializer.Serialize(json, reader.GetValue(ordinal), typeof(T));
			}
		}

		class Utf8RecordWriter : IDisposable
		{
			readonly Utf8JsonWriter json;
			readonly IDataReader reader;
			readonly JsonEncodedText[] fieldName;
			readonly Action<int>[] fieldWriter;
			readonly JsonSerializerOptions options;

			Utf8RecordWriter(Utf8JsonWriter json, JsonSerializerOptions options, IDataReader reader) {
				this.json = json;
				this.options = options;
				this.reader = reader;
				this.fieldName = new JsonEncodedText[reader.FieldCount];
				this.fieldWriter = new Action<int>[reader.FieldCount];

				for (var i = 0; i != fieldWriter.Length; ++i) {
					fieldName[i] = JsonEncodedText.Encode(reader.GetName(i));
					fieldWriter[i] = GetFieldWriter(reader.GetFieldType(i));
				}
			}

			public static Utf8RecordWriter Create(Utf8JsonWriter json, JsonSerializerOptions options, IDataReader reader) =>
				new Utf8RecordWriter(json, options, reader);

			public void Dispose() => reader.Close();

			public bool Read() => reader.Read();

			public void WriteRecord() {
				json.WriteStartObject();
				for (var i = 0; i != fieldWriter.Length; ++i)
					fieldWriter[i](i);
				json.WriteEndObject();
			}

			public Action<int> GetFieldWriter(Type t) =>
				Lambdas.CreateDelegate<Action<int>>(this,
					GetType().GetMethod(nameof(WriteField), BindingFlags.Instance | BindingFlags.NonPublic).MakeGenericMethod(t));

			void WriteField<T>(int ordinal) {
				if (reader.IsDBNull(ordinal))
					json.WriteNull(fieldName[ordinal]);
				else {
					json.WritePropertyName(fieldName[ordinal]);
					WriteValue<T>(ordinal);
				}
			}

			void WriteValue<T>(int ordinal) {
				if (typeof(T) == typeof(short))
					json.WriteNumberValue(reader.GetInt16(ordinal));
				else if (typeof(T) == typeof(int))
					json.WriteNumberValue(reader.GetInt32(ordinal));
				else if (typeof(T) == typeof(long))
					json.WriteNumberValue(reader.GetInt64(ordinal));
				else if (typeof(T) == typeof(float))
					json.WriteNumberValue(reader.GetFloat(ordinal));
				else if (typeof(T) == typeof(double))
					json.WriteNumberValue(reader.GetDouble(ordinal));
				else if (typeof(T) == typeof(decimal))
					json.WriteNumberValue(reader.GetDecimal(ordinal));
				else if (typeof(T) == typeof(bool))
					json.WriteBooleanValue(reader.GetBoolean(ordinal));
				else if (typeof(T) == typeof(string))
					json.WriteStringValue(reader.GetString(ordinal));
				else JsonSerializer.Serialize(json, reader.GetValue(ordinal), typeof(T), options);
			}
		}

		public interface IDataReaderJsonObject
		{
			void WriteRecords(NewtonsoftJsonWriter json, NewtonsoftJsonSerializer serializer);
			void WriteRecords(Utf8JsonWriter json, JsonSerializerOptions options);
		}

		[JsonConverter(typeof(DataReaderJsonConverter<DataReaderJsonArray>))]
		[NewtonsoftJsonConverter(typeof(NewtonsoftDataReaderJsonConverter))]
		public class DataReaderJsonArray : IDataReaderJsonObject
		{
			readonly IDataReader reader;

			public DataReaderJsonArray(IDataReader reader) {
				this.reader = reader;
			}

			public void WriteRecords(NewtonsoftJsonWriter json, NewtonsoftJsonSerializer serializer) {
				using var records = new NewtonsoftRecordWriter(json, serializer, reader);
				json.WriteStartArray();
				while (records.Read())
					records.WriteRecord();
				json.WriteEndArray();
			}

			public void WriteRecords(Utf8JsonWriter json, JsonSerializerOptions options = null) {
				using var records = Utf8RecordWriter.Create(json, options, reader);
				json.WriteStartArray();
				while (records.Read())
					records.WriteRecord();
				json.WriteEndArray();
			}
		}

		[JsonConverter(typeof(DataReaderJsonConverter<DataReaderJsonColumns>))]
		[NewtonsoftJsonConverter(typeof(NewtonsoftDataReaderJsonConverter))]
		public class DataReaderJsonColumns : IDataReaderJsonObject
		{
			readonly IDataReader reader;

			public DataReaderJsonColumns(IDataReader reader) {
				this.reader = reader;
			}

			public void WriteRecords(NewtonsoftJsonWriter json, NewtonsoftJsonSerializer serializer) {
				var columns = ReadColumns();
				json.WriteStartObject();
				for (var i = 0; i != reader.FieldCount; ++i) {
					json.WritePropertyName(reader.GetName(i));
					columns[i].WriteArray(json);
				}
				json.WriteEndObject();
				reader.Close();
			}

			public void WriteRecords(Utf8JsonWriter json, JsonSerializerOptions options = null) {
				var columns = ReadColumns();
				json.WriteStartObject();
				for (var i = 0; i != reader.FieldCount; ++i) {
					json.WritePropertyName(reader.GetName(i));
					columns[i].WriteArray(json, options);
				}
				json.WriteEndObject();
				reader.Close();
			}

			IDataReaderColumn[] ReadColumns() {
				var columns = new IDataReaderColumn[reader.FieldCount];
				for (var i = 0; i != columns.Length; ++i)
					columns[i] = (IDataReaderColumn)Activator.CreateInstance(typeof(DataReaderColumn<>).MakeGenericType(reader.GetFieldType(i)), i);
				while (reader.Read())
					for (var i = 0; i != columns.Length; ++i)
						columns[i].Add(reader);
				return columns;
			}
		

			interface IDataReaderColumn
			{
				void Add(IDataRecord record);
				void WriteArray(NewtonsoftJsonWriter json);
				void WriteArray(Utf8JsonWriter json, JsonSerializerOptions options);
			}

			class DataReaderColumn<T> : IDataReaderColumn
			{
				readonly List<T> values;
				readonly List<bool> hasValue;
				readonly int ordinal;

				public DataReaderColumn(int ordinal) {
					this.values = new();
					this.hasValue = new();
					this.ordinal = ordinal;
				}

				public int Count => values.Count;

				public void Add(IDataRecord record) {
					var hasValue = !record.IsDBNull(ordinal);
					if (hasValue)
						values.Add(GetValue(record));
					else values.Add(default);
					this.hasValue.Add(hasValue);
				}

				public void WriteArray(NewtonsoftJsonWriter json) {
					json.WriteStartArray();
					for (var i = 0; i != values.Count; ++i)
						if (hasValue[i])
							json.WriteValue(values[i]);
						else json.WriteNull();
					json.WriteEndArray();
				}

				public void WriteArray(Utf8JsonWriter json, JsonSerializerOptions options) {
					json.WriteStartArray();
					for (var i = 0; i != values.Count; ++i)
						if (hasValue[i])
							WriteValue(json, values[i], options);
						else json.WriteNullValue();
					json.WriteEndArray();
				}

				static void WriteValue(Utf8JsonWriter json, T value, JsonSerializerOptions options) {
					if (typeof(T) == typeof(short))
						json.WriteNumberValue((short)(object)value);
					else if (typeof(T) == typeof(int))
						json.WriteNumberValue((int)(object)value);
					else if (typeof(T) == typeof(long))
						json.WriteNumberValue((long)(object)value);
					else if (typeof(T) == typeof(float))
						json.WriteNumberValue((float)(object)value);
					else if (typeof(T) == typeof(double))
						json.WriteNumberValue((double)(object)value);
					else if (typeof(T) == typeof(decimal))
						json.WriteNumberValue((decimal)(object)value);
					else if (typeof(T) == typeof(bool))
						json.WriteBooleanValue((bool)(object)value);
					else if (typeof(T) == typeof(string))
						json.WriteStringValue((string)(object)value);
					else
						JsonSerializer.Serialize(json, value, typeof(T), options);
				}

				T GetValue(IDataRecord record) {
					if (typeof(T) == typeof(short))
						return (T)(object)record.GetInt16(ordinal);
					else if (typeof(T) == typeof(int))
						return (T)(object)record.GetInt32(ordinal);
					else if (typeof(T) == typeof(long))
						return (T)(object)record.GetInt64(ordinal);
					else if (typeof(T) == typeof(float))
						return (T)(object)record.GetFloat(ordinal);
					else if (typeof(T) == typeof(double))
						return (T)(object)record.GetDouble(ordinal);
					else if (typeof(T) == typeof(decimal))
						return (T)(object)record.GetDecimal(ordinal);
					else if (typeof(T) == typeof(bool))
						return (T)(object)record.GetBoolean(ordinal);
					else if (typeof(T) == typeof(string))
						return (T)(object)record.GetString(ordinal);
					else
						return (T)record.GetValue(ordinal);
				}
			}
		}

		public class NewtonsoftDataReaderJsonConverter : NewtonsoftJsonConverter
		{
			public override bool CanConvert(Type objectType) => typeof(IDataReaderJsonObject).IsAssignableFrom(objectType);

			public override object ReadJson(NewtonsoftJsonReader reader, Type objectType, object existingValue, NewtonsoftJsonSerializer serializer) {
				throw new NotImplementedException();
			}

			public override void WriteJson(NewtonsoftJsonWriter writer, object value, NewtonsoftJsonSerializer serializer) =>
				((IDataReaderJsonObject)value).WriteRecords(writer, serializer);
		}

		public class DataReaderJsonConverter<T> : JsonConverter<T> where T : IDataReaderJsonObject
		{
			public override T Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options) {
				throw new NotImplementedException();
			}

			public override void Write(Utf8JsonWriter writer, T value, JsonSerializerOptions options) =>
				value.WriteRecords(writer, options);
		}

		public static DataReaderJsonArray ToJsonObject(this IDataReader r) => new DataReaderJsonArray(r);
		public static DataReaderJsonColumns ToJsonColumns(this IDataReader r) => new DataReaderJsonColumns(r);

		public static string ToJson(this IDataReader r) {
			var bytes = new MemoryStream();
			using var json = new Utf8JsonWriter(bytes);
			r.ToJsonObject().WriteRecords(json);
			json.Flush();
			return bytes.TryGetBuffer(out var buffer)
			? Encoding.UTF8.GetString(buffer.Array, buffer.Offset, buffer.Count)
			: Encoding.UTF8.GetString(bytes.ToArray());
		}
	}
}
