#if MSSQLCLIENT
namespace DataBoss.Data.MsSql
{
	using Microsoft.Data.SqlClient;
	using MsSqlCommand = Microsoft.Data.SqlClient.SqlCommand;
	using MsSqlParameter = Microsoft.Data.SqlClient.SqlParameter;
#else
namespace DataBoss.Data.SqlClient
{
	using System.Data.SqlClient;
	using MsSqlCommand = System.Data.SqlClient.SqlCommand;
	using MsSqlParameter = System.Data.SqlClient.SqlParameter;
#endif

	using System;
	using System.Data;
	using System.Linq.Expressions;
	using DataBoss.Expressions;
	using DataBoss.Data.SqlServer;
	using DataBoss.Linq.Expressions;
	using System.Data.SqlTypes;
	using DataBoss.Data.Support;
	using System.Collections.Generic;
	using System.Reflection;

	public class MsSqlDialect : SqlDialect<MsSqlDialect, MsSqlCommand>, ISqlDialect
	{
		public string FormatName(string columnName) => $"[{columnName}]";
		public string GetTypeName(DataBossDbType dbType) => dbType.ToString();

		public bool TryCreateDialectSpecificParameter(string name, Expression readMember, out Expression create) {
			if (TryGetParameterPrototype(readMember.Type, out var createProto)) {
				create = Rebind(createProto, Expression.Constant(name), readMember.Box());
				return true;
			}
			else if (readMember.Type.TryGetNullableTargetType(out var valueType) && TryGetParameterPrototype(valueType, out createProto)) {
				var value = Expression.Variable(readMember.Type, "value");
				var body = Expression.Block(
					new[] { value },
					Expression.Assign(value, readMember),
					Expression.Condition(
						Expression.Property(value, "HasValue"),
						Expression.Convert(Expression.Property(value, "Value"), typeof(object)),
						Expression.Constant(DBNull.Value, typeof(object))));
				create = Rebind(createProto, Expression.Constant(name), body);
				return true;
			}

			create = null;
			return false;
		}

		public (NewExpression, PropertyInfo) CreateParameter(string name, Type type, DbType dbType) => (null, null);
		public bool SupportsNullable => false;
		public bool EnsureDBNull => true;

		public IReadOnlyList<string> DataBossHistoryMigrations => new[] {
				  "create table [dbo].[__DataBossHistory](\n"
				+ "[Id] bigint not null,\n"
				+ "[Context] varchar(64) not null,\n"
				+ "[Name] varchar(max) not null,\n"
				+ "[StartedAt] datetime not null,\n"
				+ "[FinishedAt] datetime,\n"
				+ "[User] varchar(max),\n"
				+ "constraint[PK_DataBossHistory] primary key([Id], [Context]))",

				  "alter table[dbo].[__DataBossHistory]\n"
				+ "add [MigrationHash] binary(32)",
		};

		public string BeginMigrationQuery =>
			  "update __DataBossHistory with(holdlock)\n"
			+ "set [StartedAt] = getdate(), [FinishedAt] = null, [MigrationHash] = @hash\n"
			+ "where Id = @id and Context = @context\n"
			+ "if @@rowcount = 0\n"
			+ "  insert __DataBossHistory(Id, Context, Name, StartedAt, [User], [MigrationHash])\n"
			+ "  values(@id, @context, @name, getdate(), @user, @hash)";

		public string EndMigrationQuery => "update __DataBossHistory set FinishedAt = getdate() where Id = @id and Context = @Context";

		static bool TryGetParameterPrototype(Type type, out LambdaExpression found) {
			if (type == typeof(byte[]))
				found = CreateBinaryParameter;
			else if (type == typeof(SqlDecimal))
				found = CreateSqlDecimalParameter;
			else if (type == typeof(SqlMoney))
				found = CreateSqlMoneyParameter;
			else if (type == typeof(SqlBinary))
				found = CreateSqlBinaryParameter;
			else if (type == typeof(RowVersion))
				found = CreateRowVersionParameter;
			else if (typeof(ITableValuedParameter).IsAssignableFrom(type))
				found = CreateTableValuedParameter;
			else found = null;
			return found != null;
		}

		static Expression Rebind(LambdaExpression expr, Expression arg0, Expression arg1) =>
			NodeReplacementVisitor.ReplaceParameters(expr, arg0, arg1);

		static readonly Expression<Func<string, object, MsSqlParameter>> CreateBinaryParameter =
			(name, value) => new MsSqlParameter(name, SqlDbType.Binary) { Value = value, };

		static readonly Expression<Func<string, object, MsSqlParameter>> CreateSqlDecimalParameter =
			(name, value) => new MsSqlParameter(name, SqlDbType.Decimal) { Value = value, };

		static readonly Expression<Func<string, object, MsSqlParameter>> CreateSqlMoneyParameter =
			(name, value) => new MsSqlParameter(name, SqlDbType.Money) { Value = value, };

		static readonly Expression<Func<string, object, MsSqlParameter>> CreateSqlBinaryParameter =
			(name, value) => new MsSqlParameter(name, SqlDbType.Binary) { Value = value, };

		static readonly Expression<Func<string, object, MsSqlParameter>> CreateRowVersionParameter =
			(name, value) => new MsSqlParameter(name, SqlDbType.Binary, 8) { Value = value, };

		static readonly Expression<Func<string, ITableValuedParameter, MsSqlParameter>> CreateTableValuedParameter =
			(name, value) => new MsSqlParameter(name, SqlDbType.Structured) {
				TypeName = value.TypeName,
				Value = value.Rows
			};
	}
}