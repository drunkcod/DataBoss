using System.Text.Json.Serialization;
using MongoDB.Bson;

namespace DataBoss.MongoDB;

public readonly record struct ResumeTokenTimestamp(
    [property: JsonPropertyName("t")] int Timestamp,
    [property: JsonPropertyName("i")] int Increment)
{
    public static implicit operator BsonTimestamp(ResumeTokenTimestamp self) => new(self.Timestamp, self.Increment);
    public static implicit operator ResumeTokenTimestamp?(BsonTimestamp other) => other is null ? null : new(other.Timestamp, other.Increment);
}
