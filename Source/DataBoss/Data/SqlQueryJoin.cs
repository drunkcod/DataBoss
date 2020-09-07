using System;
using System.Linq.Expressions;

namespace DataBoss.Data
{
	public class SqlQueryJoin
	{
		readonly SqlQueryFrom from;
		readonly string table;
		readonly string expr;

		internal static SqlQueryJoin Create(SqlQueryFrom from, string table, Expression<Func<bool>> expr)
		{
			var e = (BinaryExpression)expr.Body;
			var left = SqlQuery.EvalAsQueryColumn(e.Left);
			var right = SqlQuery.EvalAsQueryColumn(e.Right);
			
			return new SqlQueryJoin(from, table, $"{left} {GetBinaryOp(e.NodeType)} {right}");
		}

		static string GetBinaryOp(ExpressionType type) {
			switch(type)
			{
				default: throw new NotSupportedException($"Unsupported binary op '{type}'");
				case ExpressionType.Equal: return "=";
			}
		}

		SqlQueryJoin(SqlQueryFrom from, string table, string expr)
		{
			this.from = from;
			this.table = table;
			this.expr = expr;
		}

		public override string ToString() => ToString(SqlQueryFormatting.Default);
		public string ToString(SqlQueryFormatting formatting) => AppendTo(new SqlStringBuilder { Formatting = formatting }).ToString();

		public SqlStringBuilder AppendTo(SqlStringBuilder query) =>
			from.AppendTo(query)
			.BeginBlock("join").Space().Append(table).Space().Append("on").Space().Append(expr).EndBlock();
	}
}
