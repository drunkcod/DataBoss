using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

namespace DataBoss.Data
{
	public class SqlQueryColumn
	{
		readonly string table;
		readonly string name;

		public SqlQueryColumn(string table, string name)
		{
			this.table = table;
			this.name = name;
		}

		public override string ToString() => $"{table}.{name}";

	}
	public class SqlQueryColumn<T> : SqlQueryColumn
	{
		readonly string table;
		readonly string name;

		public SqlQueryColumn(string table, string name) : base(table, name) { }

		public static implicit operator T(SqlQueryColumn<T> self) => default(T);
	}

	public class SqlQuerySelect
	{
		readonly KeyValuePair<string, SqlQueryColumn>[] selectList;

		internal SqlQuerySelect(KeyValuePair<string, SqlQueryColumn>[] selectList) { this.selectList = selectList; }

		public override string ToString()
		{
			if (selectList.Length == 0)
				return "select *";
			return "select " + string.Join(", ", Array.ConvertAll(selectList, x => $"[{x.Key}] = {x.Value}"));
		}
	}

	public class SqlQuery
	{
		public static SqlQueryColumn<T> Column<T>(string table, string name) => new SqlQueryColumn<T>(table, name);
		public static SqlQuerySelect Select<T>(Expression<Func<T>> selector)
		{
			var init = selector.Body as MemberInitExpression;
			var body = init != null ? init.NewExpression : selector.Body as NewExpression;
			if (body == null)
				throw new NotSupportedException("Can't create select from " + selector.Body.NodeType);
			var p = body.Constructor.GetParameters();
			var q = init?.Bindings.ToArray() ?? new MemberBinding[0];
			var args = new KeyValuePair<string, SqlQueryColumn>[p.Length + q.Length];
			var n = 0;
			for (var i = 0; i != p.Length; ++i, ++n)
			{
				var m = (MethodCallExpression)((UnaryExpression)body.Arguments[i]).Operand;
				args[n] = new KeyValuePair<string, SqlQueryColumn>(p[i].Name, (SqlQueryColumn)Delegate.CreateDelegate(typeof(Func<string, string, SqlQueryColumn>), m.Method).DynamicInvoke(m.Arguments.Select(x => ((ConstantExpression)x).Value).ToArray()));
			}
			for(var i = 0; i != q.Length; ++i, ++n)
			{
				var a = ((MemberAssignment)q[i]).Expression;
				var m = (MethodCallExpression)((UnaryExpression)a).Operand;
				args[n] = new KeyValuePair<string, SqlQueryColumn>(q[i].Member.Name, (SqlQueryColumn)Delegate.CreateDelegate(typeof(Func<string, string, SqlQueryColumn>), m.Method).DynamicInvoke(m.Arguments.Select(x => ((ConstantExpression)x).Value).ToArray()));
			}
			return new SqlQuerySelect(args);
		}
	}

}
