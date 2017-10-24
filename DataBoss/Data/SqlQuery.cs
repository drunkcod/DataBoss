using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;

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

	public class SqlQueryFrom
	{
		readonly SqlQuerySelect select;
		readonly string table;

		internal SqlQueryFrom(SqlQuerySelect select, string table) { 
			this.select = select;
			this.table = table;
		}

		public override string ToString() => select.ToString() + " from " + table;
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
