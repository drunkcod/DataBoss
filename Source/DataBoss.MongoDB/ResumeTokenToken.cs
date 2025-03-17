using System.Text.Json.Serialization;

namespace DataBoss.MongoDB;

public readonly record struct ResumeTokenToken([property: JsonPropertyName("_data")] string? Data)
{ }
