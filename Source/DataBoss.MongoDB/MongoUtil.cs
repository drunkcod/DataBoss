using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Encodings.Web;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Bson.IO;
using MongoDB.Bson.Serialization;
using MongoDB.Driver;

using ChangeStreamBsonDocument = MongoDB.Driver.ChangeStreamDocument<MongoDB.Bson.BsonDocument>;

namespace DataBoss.MongoDB;

public readonly record struct ResumeTokenToken([property: JsonPropertyName("_data")] string? Data)
{ }

public struct ResumeToken
{
	[JsonPropertyName("startAtOperationTime")]
	public ResumeTokenTimestamp? StartAtOperationTime { get; set; }
	[JsonPropertyName("startAfter")]
	public ResumeTokenToken? StartAfter { get; set; }

	public ChangeStreamOptions ToChangeStreamOptions() {
		var x = new ChangeStreamOptions();
		if (StartAtOperationTime.HasValue)
			x.StartAtOperationTime = StartAtOperationTime.Value;
		if (StartAfter.HasValue)
			x.StartAfter = new BsonDocument { { "_data", StartAfter.Value.Data } };
		return x;
	}

	public static ResumeToken From(ChangeStreamOptions options) =>
		new() {
			StartAtOperationTime = options.StartAtOperationTime,
			StartAfter = options.StartAfter is null ? null : new ResumeTokenToken(options.StartAfter.GetElement("_data").Value.AsString),
		};
}

public readonly record struct ResumeTokenTimestamp(
	[property: JsonPropertyName("t")] int Timestamp,
	[property: JsonPropertyName("i")] int Increment)
{
	public static implicit operator BsonTimestamp(ResumeTokenTimestamp self) => new(self.Timestamp, self.Increment);
	public static implicit operator ResumeTokenTimestamp?(BsonTimestamp other) => other is null ? null : new(other.Timestamp, other.Increment);
}

public static class MongoUtil
{
	public static void WriteObjectId(ObjectId value, Utf8JsonWriter writer) {
		var alphabet = "0123456789abcdef"u8;
		var hex = new byte[24];
		value.ToByteArray(hex, 12);
		int i = 12, n = 0;
		do {
			hex[n++] = alphabet[hex[i] >> 4];
			hex[n++] = alphabet[hex[i] & 0xf];
		} while (++i != 24);
		writer.WriteStringValue(hex);
	}

	public static BsonValue? WriteJson(BsonReader bson, Utf8JsonWriter json) {
		void WriteArray(int depth) {
			bson.ReadStartArray();
			json.WriteStartArray();
			for (; ; ) {
				bson.ReadBsonType();
				if (bson.State == BsonReaderState.EndOfArray)
					break;
				WriteValue(depth + 1);
			}
			bson.ReadEndArray();
			json.WriteEndArray();
		}

		void WriteValue(int depth) {
			switch (bson.CurrentBsonType) {
				default: throw new NotSupportedException($"{bson.CurrentBsonType}");
				case BsonType.ObjectId: WriteObjectId(bson.ReadObjectId(), json); break;
				case BsonType.Binary: json.WriteBase64StringValue(bson.ReadBinaryData().Bytes); break;
				case BsonType.String: json.WriteStringValue(bson.ReadString().Replace("\0", string.Empty)); break;
				case BsonType.DateTime:
					var v = BsonConstants.UnixEpoch.AddMilliseconds(bson.ReadDateTime());
					json.WriteStringValue(v.ToString("O"));
					break;
				case BsonType.Timestamp:
					json.WriteStartObject();
					var t = bson.ReadTimestamp();
					json.WriteNumber("t", t >> 32);
					json.WriteNumber("i", (int)t);
					json.WriteEndObject();
					break;
				case BsonType.Boolean: json.WriteBooleanValue(bson.ReadBoolean()); break;
				case BsonType.Int32: json.WriteNumberValue(bson.ReadInt32()); break;
				case BsonType.Int64: json.WriteNumberValue(bson.ReadInt64()); break;
				case BsonType.Double:
					var value = bson.ReadDouble();
					try {
						json.WriteNumberValue(value);
					}
					catch (ArgumentException) {
						if (double.IsNaN(value)) {
							json.WriteStringValue("NaN");
						}
						else throw;
					}
					break;
				case BsonType.Decimal128: json.WriteRawValue(bson.ReadDecimal128().ToString()); break;
				case BsonType.Null: bson.ReadNull(); json.WriteNullValue(); break;

				case BsonType.Document: WriteDocument(depth + 1); break;
				case BsonType.Array: WriteArray(depth); break;
			}
		}

		BsonValue ReadValue() {
			switch (bson.CurrentBsonType) {
				default: throw new NotSupportedException($"{bson.CurrentBsonType}");
				case BsonType.ObjectId: return bson.ReadObjectId();
				case BsonType.String: return bson.ReadString().Replace("\0", string.Empty);
				case BsonType.Document: return new RawBsonDocument(bson.ReadRawBsonDocument());
			}
		}

		BsonValue? WriteDocument(int depth = 0) {
			bson.ReadStartDocument();
			json.WriteStartObject();
			BsonValue? id = null;
			while (bson.ReadBsonType() != BsonType.EndOfDocument) {
				var p = bson.ReadName();
				json.WritePropertyName(p);
				if (depth == 0 && p == "_id") {
					var mark = bson.GetBookmark();
					WriteValue(depth);
					bson.ReturnToBookmark(mark);
					id = ReadValue();
				}
				else {
					WriteValue(depth);
				}
			}
			bson.ReadEndDocument();
			json.WriteEndObject();
			return id;
		}
		try {
			return WriteDocument();
		}
		finally {
			json.Flush();
		}
	}

	public static BsonValue? RawWriteUtf8(RawBsonDocument b, Stream s) {
		using var json = new Utf8JsonWriter(s, new JsonWriterOptions {
			Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping,
		});
		using var bson = new BsonBinaryReader(new ByteBufferStream(b.Slice));
		return WriteJson(bson, json);
	}

	public static (int Changes, (BsonValue Id, DateTime ChangedAt)[] Documents, BsonDocument? Resumetoken) GetChangedDocuments(IMongoCollection<BsonDocument> collection, ChangeStreamOptions options) {
		var unset = new BsonDocument { { "$unset", new BsonArray { "fullDocument", "updateDescription" } } };
		var pipeline = new EmptyPipelineDefinition<ChangeStreamBsonDocument>()
			.AppendStage<ChangeStreamBsonDocument, ChangeStreamBsonDocument, ChangeStreamBsonDocument>(unset);

		using var cur = collection.Watch(pipeline, options);

		var rows = new Dictionary<BsonValue, DateTime>();
		var n = 0;
		BsonDocument? resumeToken = null;
		while (cur.MoveNext()) {
			using var it = cur.Current.GetEnumerator();
			if (!it.MoveNext()) break;
			do {
				++n;
				rows[it.Current.DocumentKey.GetValue("_id")] = it.Current.WallTime ?? BsonConstants.UnixEpoch.AddSeconds(it.Current.ClusterTime.AsBsonTimestamp.Timestamp);
				resumeToken = it.Current.ResumeToken;
			} while (it.MoveNext());
		}

		return (n, rows.Select(x => (x.Key, x.Value)).ToArray(), resumeToken);
	}
}

public static class ChannelExtensions
{
	class SelectChannelReader<T, TOut> : ChannelReader<TOut>
	{
		readonly ChannelReader<T> inner;
		readonly Func<T, TOut> selector;

		public SelectChannelReader(ChannelReader<T> inner, Func<T, TOut> selector) {
			this.inner = inner;
			this.selector = selector;
		}

		public override bool TryRead([MaybeNullWhen(false)] out TOut item) {
			if (inner.TryRead(out var found)) {
				item = selector(found);
				return true;
			}
			item = default;
			return false;
		}

		public override ValueTask<bool> WaitToReadAsync(CancellationToken cancellationToken = default) => inner.WaitToReadAsync(cancellationToken);
	}

	public static ChannelReader<TOut> Select<T, TOut>(this ChannelReader<T> reader, Func<T, TOut> transform) => new SelectChannelReader<T, TOut>(reader, transform);
}

public static class IAsyncCursorExtensions
{
	interface IRawBsonDocuemtDeserializer<T>
	{
		T Deserialize(RawBsonDocument document);
	}

	class RawBsonDocumentNopDeserializer : IRawBsonDocuemtDeserializer<RawBsonDocument>
	{
		private RawBsonDocumentNopDeserializer() { }
		public RawBsonDocument Deserialize(RawBsonDocument document) => document;
		public static readonly RawBsonDocumentNopDeserializer Instance = new();
	}

	class RawBsonDocumentSerializer<T> : IRawBsonDocuemtDeserializer<T>
	{
		readonly IBsonSerializer<T> bson = BsonSerializer.LookupSerializer<T>();
		public bool AllowDuplicateElementNames { get; set; }
		public T Deserialize(RawBsonDocument document) {
			using var stream = new BsonBinaryReader(new ByteBufferStream(document.Slice));
			var ctx = BsonDeserializationContext.CreateRoot(stream, x => {
				x.AllowDuplicateElementNames = AllowDuplicateElementNames;
			});

			return bson.Deserialize(ctx);
		}
	}

	public static async Task<int> WriteTo<T>(this IAsyncCursor<RawBsonDocument> xs, ChannelWriter<T> writer, CancellationToken cancellationToken = default) {
		try {
			var bson = GetDeserializer<T>();
			var n = 0;
			for (; await xs.MoveNextAsync(cancellationToken); ++n)
				foreach (var item in xs.Current) {
					var x = bson.Deserialize(item);
					while (!writer.TryWrite(x))
						if (!await writer.WaitToWriteAsync(cancellationToken))
							return n;
				}
			return n;
		}
		finally {
			xs.Dispose();
		}
	}

	static IRawBsonDocuemtDeserializer<T> GetDeserializer<T>() =>
		typeof(T) == typeof(RawBsonDocument)
		? (IRawBsonDocuemtDeserializer<T>)(object)RawBsonDocumentNopDeserializer.Instance
		: new RawBsonDocumentSerializer<T>();
}

