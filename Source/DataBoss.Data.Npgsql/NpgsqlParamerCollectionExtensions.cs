using Npgsql;
using NpgsqlTypes;

namespace DataBoss.Data
{
	public static class NpgsqlParamerCollectionExtensions
	{
		public static void AddJson<T>(this NpgsqlParameterCollection self, string name, T value) =>
			self.Add(Utf8JsonParameter(name, NpgsqlDbType.Json, value));

		public static void AddJsonb<T>(this NpgsqlParameterCollection self, string name, T value) =>
			self.Add(Utf8JsonParameter(name, NpgsqlDbType.Jsonb, value));

		static NpgsqlParameter<byte[]> Utf8JsonParameter<T>(string name, NpgsqlDbType type, T value) =>
			new (name, type) {
				TypedValue = System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(value),
			};
	}
}
