using System;
using System.IO;
using System.Text;
using System.Text.Json;
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
			ResumeTokenTimestamp? fromNull = (BsonTimestamp?)null;
			Check.That(() => fromNull.HasValue == false);
		}

		[Fact]
		public void ObjectId_ToHexString() {
			var oid = ObjectId.GenerateNewId();
			var ms = new MemoryStream();
			var json = new Utf8JsonWriter(ms);
			MongoUtil.WriteObjectId(oid, json);
			json.Flush();
			Check.That(() => ms.ToStringUtf8() == $"\"{oid}\"");
		}

		static string ToJson<T>(T value) =>
			JsonSerializer.Serialize(value);

		static ResumeTokenTimestamp FromJson(string timestamp) =>
			JsonSerializer.Deserialize<ResumeTokenTimestamp>(timestamp);
	}

	static class MemoryStreamExtensions
	{
		public static string ToStringUtf8(this MemoryStream ms) {
			if (!ms.TryGetBuffer(out var bs))
				throw new NotSupportedException("Failed to get buffer.");
			return Encoding.UTF8.GetString(bs);
		}
	}
}