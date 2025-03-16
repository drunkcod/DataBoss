using Npgsql;
using Xunit;
using CheckThat;

namespace DataBoss.Data.Npgsql
{
	public class ToParams_NpgsqlCommand : ToParamsFixture<NpgsqlCommand, NpgsqlParameter>
	{
		protected override NpgsqlCommand NewCommand() => new();
		protected override ISqlDialect SqlDialect => NpgsqlDialect.Instance;

		[Fact]
		public void enumerable_as_array() => Check
			.With(() => GetParams(new { xs = new[] { 1, 2, 3 }.Select(x => x).AsEnumerable() }))
			.That(
				paras => (paras[0].NpgsqlDbType & NpgsqlTypes.NpgsqlDbType.Array) == NpgsqlTypes.NpgsqlDbType.Array,
				paras => paras[0].Value!.GetType() == typeof(int[]));
	}
}