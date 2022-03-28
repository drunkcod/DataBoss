using System;
using System.Data;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using System.Linq;
using System.Reflection;
using CheckThat;
using DataBoss.Data;
using DataBoss.Data.SqlServer;
using Xunit;

namespace DataBoss
{
	public abstract class ToParamsFixture<TCommand, TParameter> 
		where TCommand : IDbCommand 
		where TParameter : IDbDataParameter
	{
		protected abstract TCommand NewCommand();
		protected abstract ISqlDialect SqlDialect { get; }

		protected TParameter[] GetParams<T>(T args) { 
			var cmd = NewCommand();
			ToParams.CompileExtractor<TCommand, T>(SqlDialect)(cmd, args);
			return cmd.Parameters.Cast<TParameter>().ToArray();
		}

		[Fact]
		public void nullables() {
			var p = GetParams(new {
				Int32_Value = (int?)1,
				Int32_Null = (int?)null,
				String_Null = (string)null,
			});

			Check.That(
				() => p.Single(x => x.ParameterName == ParameterName("Int32_Value")).DbType == DbType.Int32,
				() => p.Single(x => x.ParameterName == ParameterName("Int32_Null")).DbType == DbType.Int32,
				() => p.Single(x => x.ParameterName == ParameterName("String_Null")).DbType == DbType.String);
		}

		[Fact]
		public void null_string() => Check
			.With(() => GetParams(new { NullString = (string)null }))
			.That(
				xs => xs.Length == 1,
				xs => xs[0].Value == DBNull.Value);

		[Fact]
		public void Uri_is_treated_as_string() {
			var uri = new Uri("http://example.com");
			Check.With(() =>
			GetParams(new {
				Uri = uri,
				NullUri = (Uri)null,
			})).That(
				xs => xs.Length == 2,
				xs => xs[0].Value.Equals(uri.ToString()),
				xs => xs[1].Value == DBNull.Value);
		}

		class MyRow { }

		[Fact]
		public void IdOf_as_int() {
			var x = GetParams(new { Id = new IdOf<MyRow>(1) });
			Check.That(() => x.Length == 1);
			Check.That(
				() => x.Length == 1,
				() => x[0].ParameterName == ParameterName("Id"),
				() => x[0].DbType== DbType.Int32);
		}

		[Fact]
		public void complex_type() => Check
			.With(() => GetParams(new { Args = new { Foo = 1, Bar = "Hello" } }))
			.That(
				x => x.Length == 2,
				x => x.Any(p => p.ParameterName == ParameterName("Args_Foo")),
				x => x.Any(p => p.ParameterName == ParameterName("Args_Bar")));

		string ParameterName(string name) => $"{SqlDialect.ParameterPrefix}{name}";
	}

	public class ToParams_SqlCommand : ToParamsFixture<SqlCommand, SqlParameter>
	{
		protected override SqlCommand NewCommand() => new();
		protected override ISqlDialect SqlDialect => MsSqlDialect.Instance;

		[Theory]
		[InlineData(typeof(string))]
		[InlineData(typeof(Guid))]
		[InlineData(typeof(DateTime))]
		[InlineData(typeof(decimal))]
		[InlineData(typeof(byte[]))]
		public void has_sql_type_mapping_for(Type clrType) => 
			Check.That(() => ToParams.HasSqlTypeMapping(clrType));

		[Fact]
		public void object_is_not_considered_complex() {
			var nullableInt = new int?();
			Check.With(() => GetParams(new { Value = nullableInt.HasValue ? (object)nullableInt.Value : DBNull.Value }))
				.That(x => x.Length == 1, x => x.Any(p => p.ParameterName == "@Value"));
		}

		[Theory]
		[InlineData(typeof(int), SqlDbType.Int)]
		[InlineData(typeof(decimal), SqlDbType.Decimal)]
		[InlineData(typeof(SqlDecimal), SqlDbType.Decimal)]
		[InlineData(typeof(RowVersion), SqlDbType.Binary)]
		public void nullable_values(Type type, SqlDbType sqlDbType) => 
			GetType().GetMethod(nameof(CheckNullable), BindingFlags.Instance | BindingFlags.NonPublic)
			.MakeGenericMethod(type).Invoke(this, new object[]{ sqlDbType });

		void CheckNullable<T>(SqlDbType sqlDbType) where T : struct => Check.With(() =>
			GetParams(new {
				HasValue = new T?(default),
				IsNull = new T?(),
			}))
			.That(
				xs => xs.Length == 2,
				xs => xs[0].Value.Equals(default(T)),
				xs => xs[0].SqlDbType == sqlDbType,
				xs => xs[1].Value == DBNull.Value,
				xs => xs[1].SqlDbType == sqlDbType);

		[Fact]
		public void RowVersion_as_SqlBinary_value() => Check
			.With(() => GetParams(new { RowVersion = new RowVersion(new byte[8])}))
			.That(
				paras => paras.Length == 1,
				paras => paras[0].ParameterName == "@RowVersion",
				paras => paras[0].SqlDbType == SqlDbType.Binary);
	}

	public class ToParams_IDbCommand : ToParamsFixture<IDbCommand, IDbDataParameter>
	{
		protected override IDbCommand NewCommand() => new SqlCommand();
		protected override ISqlDialect SqlDialect => MsSqlDialect.Instance;
	}
}
