using System;
using System.Data;
using System.IO;
using System.Reflection;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using DataBoss.Data.DataFrames;

namespace DataBoss.Data
{
	using NewtonsoftJsonConverter = Newtonsoft.Json.JsonConverter;
	using NewtonsoftJsonConverterAttribute = Newtonsoft.Json.JsonConverterAttribute;
	using NewtonsoftJsonReader = Newtonsoft.Json.JsonReader;
	using NewtonsoftJsonSerializer = Newtonsoft.Json.JsonSerializer;
	using NewtonsoftJsonWriter = Newtonsoft.Json.JsonWriter;

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
				var df = DataFrame.Create(reader);
				json.WriteStartObject();
				foreach(var c in df.Columns) {
					json.WritePropertyName(c.Name);
					serializer.Serialize(json, c);
				}
				json.WriteEndObject();
				reader.Close();
			}

			public void WriteRecords(Utf8JsonWriter json, JsonSerializerOptions options = null) {
				var df = DataFrame.Create(reader);
				json.WriteStartObject();
				foreach(var c in df.Columns) {
					json.WritePropertyName(c.Name);
					JsonSerializer.Serialize(json, c, c.GetType(), options);
				}
				json.WriteEndObject();
				reader.Close();
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
