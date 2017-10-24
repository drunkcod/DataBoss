using System;
using System.Collections.Generic;
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
		public string ToString(SqlQueryFormatting formatting) => select.ToString(formatting) + " from " + table;
	}

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
		public string ToString(SqlQueryFormatting formatting) => from.ToString(formatting) + " join " + table + " on " + expr;

	}

	public enum SqlQueryFormatting
	{
		Default,
		Indented,
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
			if(ctor.Constructor == null)
				return Enumerable.Empty<KeyValuePair<string, SqlQueryColumn>>();
			var p = ctor.Constructor.GetParameters(); 
			return Enumerable.Range(0, p.Length).SelectMany(n => CreateChildBinding(context + p[n].Name, ctor.Arguments[n]));			
		}

		static IEnumerable<KeyValuePair<string, SqlQueryColumn>> CreateBindings(string context, MemberInitExpression init) =>
			CreateBindings(context, init.NewExpression)
			.Concat(init.Bindings.SelectMany(x => CreateChildBinding(context + x.Member.Name, (x as MemberAssignment).Expression)));

		static IEnumerable<KeyValuePair<string, SqlQueryColumn>> CreateChildBinding(string name, Expression a) {
			if (HasChildBindings(a))
				foreach (var child in CreateBindings(name + ".", a))
					yield return child;
			else
				yield return CreateBinding(name, a);
		}

		static bool HasChildBindings(Expression a) => a.NodeType == ExpressionType.MemberInit || a.NodeType == ExpressionType.New;

		internal static SqlQueryColumn EvalAsQueryColumn(Expression expr) {
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
