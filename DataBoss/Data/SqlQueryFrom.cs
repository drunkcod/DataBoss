using System;
using System.Linq.Expressions;

namespace DataBoss.Data
{
	public class SqlQueryFrom
	{
		readonly SqlQuerySelect select;
		readonly string table;

		internal SqlQueryFrom(SqlQuerySelect select, string table) { 
			this.select = select;
			this.table = table;
		}

		public SqlQueryJoin Join(string table, Expression<Func<bool>> expr) => SqlQueryJoin.Create(this, table, expr);

		public override string ToString() => ToString(SqlQueryFormatting.Default);
		public string ToString(SqlQueryFormatting formatting) => AppendTo(new SqlStringBuilder { Formatting = formatting }).ToString();

		public SqlStringBuilder AppendTo(SqlStringBuilder query) =>
			select.AppendTo(query)
			.BeginBlock("from").Space().Append(table).EndBlock();
	}
}
