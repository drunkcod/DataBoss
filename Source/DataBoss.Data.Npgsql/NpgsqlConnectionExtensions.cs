using Npgsql;
using System.Threading.Tasks;

namespace DataBoss.Data.Npgsql
{
	public static class NpgsqlConnectionExtensions
	{
		public static Task<int> ExecuteNonQueryAsync(this NpgsqlConnection connection, string sql) {
			using var c = new NpgsqlCommand(sql, connection);
			return c.ExecuteNonQueryAsync();
		}

		public static Task<int> ExecuteNonQueryAsync<T>(this NpgsqlConnection connection, string sql, T args) {
			using var c = new NpgsqlCommand(sql, connection);
			NpgsqlDialect.AddParameters(c, args);
			return c.ExecuteNonQueryAsync();
		}

		public static Task<object?> ExecuteScalarAsync(this NpgsqlConnection connection, string sql) {
			using var c = new NpgsqlCommand(sql, connection);
			return c.ExecuteScalarAsync();
		}

		public static Task<object?> ExecuteScalarAsync<T>(this NpgsqlConnection connection, string sql, T args) {
			using var c = new NpgsqlCommand(sql, connection);
			NpgsqlDialect.AddParameters(c, args);
			return c.ExecuteScalarAsync();
		}
	}
}
