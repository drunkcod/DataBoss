using System.Text.Json.Serialization;
using MongoDB.Bson;
using MongoDB.Driver;

namespace DataBoss.MongoDB;

public struct ResumeToken
{
    [JsonPropertyName("startAtOperationTime")]
    public ResumeTokenTimestamp? StartAtOperationTime { get; set; }
    [JsonPropertyName("startAfter")]
    public ResumeTokenToken? StartAfter { get; set; }

    public ChangeStreamOptions ToChangeStreamOptions()
    {
        var x = new ChangeStreamOptions();
        if (StartAtOperationTime.HasValue)
            x.StartAtOperationTime = StartAtOperationTime.Value;
        if (StartAfter.HasValue)
            x.StartAfter = new BsonDocument { { "_data", StartAfter.Value.Data } };
        return x;
    }

    public static ResumeToken From(ChangeStreamOptions options) =>
        new()
        {
            StartAtOperationTime = options.StartAtOperationTime,
            StartAfter = options.StartAfter is null ? null : new ResumeTokenToken(options.StartAfter.GetElement("_data").Value.AsString),
        };
}
