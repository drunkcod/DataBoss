using System;
using System.Collections.Generic;
using System.Text;

namespace DataBoss.Data
{
	public class SqlQuerySelect
	{
		readonly KeyValuePair<string, SqlQueryColumn>[] selectList;

		internal SqlQuerySelect(KeyValuePair<string, SqlQueryColumn>[] selectList) { this.selectList = selectList; }

		public SqlQueryFrom From(string table) => new SqlQueryFrom(this, table);

		public override string ToString() => ToString(SqlQueryFormatting.Default);

		public string ToString(SqlQueryFormatting formatting) {
			var query = new StringBuilder("select");
			var sep = formatting == SqlQueryFormatting.Default 
				? new { Begin = " ", End = string.Empty}
				: new { Begin = "\n\t", End = "\n" };
			if (selectList.Length == 0)
				return "select *";
			return query
				.Append(sep.Begin)
				.Append(string.Join("," + sep.Begin, Array.ConvertAll(selectList, x => $"[{x.Key}] = {x.Value}")))
				.Append(sep.End)
				.ToString();
		}
	}
}
