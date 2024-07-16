using System.Text.Json;
using System.Text.Json.Serialization;
using CheckThat;
using MongoDB.Bson;
using Xunit;

namespace DataBoss.MongoDB
{
	public class MongoDBUtil
	{
        [Fact]
        public void ResumeTokenTimestamp_json() =>
            Check.That(
                () => ToJson(new ResumeTokenTimestamp(1, 2)) == "{\"t\":1,\"i\":2}",
                () => new ResumeTokenTimestamp(2, 0) == FromJson(ToJson(new ResumeTokenTimestamp(2, 0))));

		[Fact]
		public void ResumeTokenToken_json() => 
			Check.That(() => ToJson(new ResumeTokenToken("data")) == "{\"_data\":\"data\"}");

		[Fact]
		public void ResumeTokenTimestamp_from_null_BsonTimeStamp() {
			ResumeTokenTimestamp? fromNull = (BsonTimestamp)null;
			Check.That(() => fromNull.HasValue == false);
		}

        static string ToJson<T>(T value) =>		
            JsonSerializer.Serialize(value);

        static ResumeTokenTimestamp FromJson(string timestamp) =>
            JsonSerializer.Deserialize<ResumeTokenTimestamp>(timestamp);
	}
}