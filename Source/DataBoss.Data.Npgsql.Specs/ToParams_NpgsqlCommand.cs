using Npgsql;

namespace DataBoss.Data.Npgsql
{
	public class ToParams_NpgsqlCommand : ToParamsFixture<NpgsqlCommand, NpgsqlParameter>
	{
		protected override NpgsqlCommand NewCommand() => new();
		protected override ISqlDialect SqlDialect => NpgsqlDialect.Instance;

	}
}