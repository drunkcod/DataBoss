using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

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
		public static SqlQuerySelect Select<T>(Expression<Func<T>> selector) => new SqlQuerySelect(CreateBindings(string.Empty, selector.Body).ToArray());

		static IEnumerable<KeyValuePair<string, SqlQueryColumn>> CreateBindings(string context, Expression expr) {
			switch(expr.NodeType) {
				default: throw new NotSupportedException("Can't create select from " + expr);
				case ExpressionType.New: return CreateBindings(context, (NewExpression)expr);
				case ExpressionType.MemberInit: return CreateBindings(context, (MemberInitExpression)expr);
			}
		}

		static IEnumerable<KeyValuePair<string, SqlQueryColumn>> CreateBindings(string context, NewExpression ctor) {
			var p = ctor.Constructor.GetParameters();
			for (var i = 0; i != p.Length; ++i)
				yield return CreateBinding(context + p[i].Name, ctor.Arguments[i]);
		}

		static IEnumerable<KeyValuePair<string, SqlQueryColumn>> CreateBindings(string context, MemberInitExpression init) {
			foreach(var item in CreateBindings(context, init.NewExpression))
				yield return item;

			var q = init.Bindings.ToArray();
			for (var i = 0; i != q.Length; ++i) {
				var name = q[i].Member.Name;
				var a = ((MemberAssignment)q[i]).Expression;
				if(a.NodeType == ExpressionType.MemberInit || a.NodeType == ExpressionType.New)
					foreach(var subitem in CreateBindings(context + name + ".", a))
						yield return subitem;
				else 
					yield return CreateBinding(context + name, a);
			}
		}

		static SqlQueryColumn EvalAsQueryColumn(Expression expr) {
			if(expr.NodeType == ExpressionType.MemberAccess) {
				var m = (MemberExpression)expr;
				return (SqlQueryColumn)((FieldInfo)m.Member).GetValue(((ConstantExpression)m.Expression).Value);
			} else { 
				var m = (MethodCallExpression)((UnaryExpression)expr).Operand;
				return (SqlQueryColumn)Delegate.CreateDelegate(typeof(Func<string, string, SqlQueryColumn>), m.Method).DynamicInvoke(m.Arguments.Select(x => ((ConstantExpression)x).Value).ToArray());
			}
		}

		static KeyValuePair<string, SqlQueryColumn> CreateBinding(string name, Expression columnBinding) =>
			new KeyValuePair<string, SqlQueryColumn>(name, EvalAsQueryColumn(columnBinding));
	}
}
