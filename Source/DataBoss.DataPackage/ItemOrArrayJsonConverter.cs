using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Newtonsoft.Json;

namespace DataBoss.DataPackage
{
	public class ItemOrArrayJsonConverter : JsonConverter
	{
		delegate object ObjectReader(JsonReader reader, object existingValue, JsonSerializer serializer);

		public override bool CanConvert(Type objectType) => typeof(IEnumerable<>).IsAssignableFrom(objectType);

		public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer) {
			var itemType = GetItemType(objectType);
			return objectType.IsArray
			? ReadArray(itemType, reader, serializer)
			: ReadCollection(itemType, reader, 
				existingValue ?? (objectType.IsAbstract ? null : Activator.CreateInstance(objectType)),
				serializer);
		}

		static Type GetItemType(Type collectionType) =>
			(collectionType.GetInterface(typeof(IEnumerable<>).Name) ?? collectionType).GetGenericArguments()[0];

		static object ReadArray(Type itemType, JsonReader reader, JsonSerializer serializer) =>
			GetObectReader(nameof(DoReadArray), itemType)(reader, null, serializer);

		static T[] DoReadArray<T>(JsonReader reader, object existingValue, JsonSerializer serializer) =>
			DoReadCollection<T>(reader, existingValue, serializer).ToArray();

		static object ReadCollection(Type itemType, JsonReader reader, object existingValue, JsonSerializer serializer) =>
			GetObectReader(nameof(DoReadCollection), itemType)(reader, existingValue, serializer);

		static IEnumerable<T> DoReadCollection<T>(JsonReader reader, object existingValue, JsonSerializer serializer) {
			var target = (existingValue as ICollection<T>) ?? new List<T>();
			void ReadItem() => target.Add(serializer.Deserialize<T>(reader));

			if(reader.TokenType == JsonToken.None && !reader.Read())
				return null;

			if (reader.TokenType == JsonToken.StartArray)
				while (reader.Read() && reader.TokenType != JsonToken.EndArray)
					ReadItem();
			else ReadItem();
			
			return target;
		}

		static ObjectReader GetObectReader(string name, Type type) =>
			(ObjectReader)Delegate.CreateDelegate(typeof(ObjectReader), GetGenericMethod(name, type));

		static MethodInfo GetGenericMethod(string name, Type type) => 
			typeof(ItemOrArrayJsonConverter).GetMethod(name, BindingFlags.Static | BindingFlags.NonPublic)
			.MakeGenericMethod(type);

		public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer) {
			if (value == null) {
				writer.WriteNull();
				return;
			}
			var items = ((IEnumerable)value).GetEnumerator();
			try {
				if (!items.MoveNext()) {
					writer.WriteStartArray();
					writer.WriteEndArray();
				}
				var first = items.Current;
				if (items.MoveNext()) {
					writer.WriteStartArray();
					serializer.Serialize(writer, first);

					do {
						serializer.Serialize(writer, items.Current);
					} while (items.MoveNext());

					writer.WriteEndArray();
				}
				else serializer.Serialize(writer, first);
			} finally {
				(items as IDisposable)?.Dispose();
			}
		}
	}
}
