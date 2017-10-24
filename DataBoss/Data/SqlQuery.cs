using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace DataBoss.Data
{
	public enum SqlQueryColumnType
	{
		TableColumn,
		Query,
	}

	public class SqlQueryColumn
	{
		readonly SqlQueryColumnType columnType;
		readonly object stuff;

		protected SqlQueryColumn(SqlQueryColumnType columnType, object stuff) {
			this.columnType = columnType;
			this.stuff = stuff;
		}

		protected SqlQueryColumn(string table, string column) {
			this.columnType = SqlQueryColumnType.TableColumn;
			this.stuff = new [] { table, column };
		}

		public static SqlQueryColumn TableColumn(string table, string column) => new SqlQueryColumn(table, column);
		public static SqlQueryColumn Query(string query) => new SqlQueryColumn(SqlQueryColumnType.Query, query);

		public override string ToString() {
			switch(columnType) { 
				case SqlQueryColumnType.Query: return (string)stuff;
				case SqlQueryColumnType.TableColumn: 
					var parts = (string[])stuff;
					return $"{parts[0]}.{parts[1]}";
			}
			throw new InvalidOperationException();
		}

	}
	public class SqlQueryColumn<T> : SqlQueryColumn
	{
		public SqlQueryColumn(string table, string name) : base(table, name) { }
		SqlQueryColumn(string query) : base(SqlQueryColumnType.Query, query) { }

		public static SqlQueryColumn<T> Null { 
			get {
				var dbType = DataBossDbType.ToDbType(typeof(T));
				if(!dbType.IsNullable)
					throw new InvalidOperationException("Column is not nullable");
				return new SqlQueryColumn<T>($"cast(null as {dbType})");
			}
		}

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
		public static SqlQueryColumn<T> Null<T>() => SqlQueryColumn<T>.Null;
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
			switch(expr.NodeType)
			{
				default: throw new NotSupportedException($"Unsupported NodeType '{expr.NodeType}'");
				case ExpressionType.MemberAccess: return (SqlQueryColumn)GetMember(expr as MemberExpression);
				case ExpressionType.Convert: return EvalAsQueryColumn((expr as UnaryExpression).Operand);
				case ExpressionType.Call:
					var m = expr as MethodCallExpression;
					return (SqlQueryColumn)m.Method.Invoke(null, m.Arguments.Select(x => ((ConstantExpression)x).Value).ToArray());
			}
		}

		static object GetMember(MemberExpression m) {
			switch(m.Member.MemberType) { 
				default: throw new NotSupportedException($"Unsupported MemberType '{m.Member.MemberType}'");
				case MemberTypes.Field: return ((FieldInfo)m.Member).GetValue(((ConstantExpression)m.Expression).Value);
				case MemberTypes.Property: 
					var prop = m.Member as PropertyInfo;
					if(m.Expression == null)
						return prop.GetValue(null);
					return prop.GetValue(((ConstantExpression)m.Expression).Value);
			}
		}

		static KeyValuePair<string, SqlQueryColumn> CreateBinding(string name, Expression columnBinding) =>
			new KeyValuePair<string, SqlQueryColumn>(name, EvalAsQueryColumn(columnBinding));
	}
}
