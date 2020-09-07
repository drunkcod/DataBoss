using System;
using System.Collections.Generic;

namespace DataBoss.Data
{
	public class SqlQuerySelect
	{
		readonly KeyValuePair<string, SqlQueryColumn>[] selectList;

		internal SqlQuerySelect(KeyValuePair<string, SqlQueryColumn>[] selectList) { this.selectList = selectList; }

		public SqlQueryFrom From(string table) => new SqlQueryFrom(this, table);

		public override string ToString() => ToString(SqlQueryFormatting.Default);
		public string ToString(SqlQueryFormatting formatting) => AppendTo(new SqlStringBuilder {  Formatting = formatting }).ToString();

		public SqlStringBuilder AppendTo(SqlStringBuilder query) {
			query.BeginBlock("select").EndElement().BeginIndent();
			if (selectList.Length == 0)
				query.Append("*");
			else { 
				var parts = Array.ConvertAll(selectList, x => $"[{x.Key}] = {x.Value}");
				query.Begin(parts[0]);
				for(var i = 1; i != parts.Length; ++i)
					query.Append(",").EndElement().Begin(parts[i]);
			}
			return	query.EndIndent().EndBlock();
		}
	}
}
