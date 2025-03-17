using System.Text.Json;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Bson.IO;
using MongoDB.Bson.Serialization;
using MongoDB.Driver;

namespace DataBoss.MongoDB;

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
        public T Deserialize(RawBsonDocument document)
        {
            using var stream = new BsonBinaryReader(new ByteBufferStream(document.Slice));
            var ctx = BsonDeserializationContext.CreateRoot(stream, x =>
            {
                x.AllowDuplicateElementNames = AllowDuplicateElementNames;
            });

            return bson.Deserialize(ctx);
        }
    }

    public static async Task<int> WriteTo<T>(this IAsyncCursor<RawBsonDocument> xs, ChannelWriter<T> writer, CancellationToken cancellationToken = default)
    {
        try
        {
            var bson = GetDeserializer<T>();
            var n = 0;
            for (; await xs.MoveNextAsync(cancellationToken); ++n)
                foreach (var item in xs.Current)
                {
                    var x = bson.Deserialize(item);
                    while (!writer.TryWrite(x))
                        if (!await writer.WaitToWriteAsync(cancellationToken))
                            return n;
                }
            return n;
        }
        finally
        {
            xs.Dispose();
        }
    }

    static IRawBsonDocuemtDeserializer<T> GetDeserializer<T>() =>
        typeof(T) == typeof(RawBsonDocument)
        ? (IRawBsonDocuemtDeserializer<T>)(object)RawBsonDocumentNopDeserializer.Instance
        : new RawBsonDocumentSerializer<T>();
}

