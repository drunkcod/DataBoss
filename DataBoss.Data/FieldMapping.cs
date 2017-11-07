using DataBoss.Linq;
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace DataBoss.Data
{
	public class FieldMapping<T>
	{
		struct FieldMappingItem
		{
			public string Name;
			public Type FieldType;
			public Expression Selector;
			public DataBossDbType DbType;
		}

		readonly ParameterExpression source = Expression.Parameter(typeof(T), "source");
		readonly ParameterExpression target = Expression.Parameter(typeof(object[]), "target");

		readonly List<FieldMappingItem> mappings = new List<FieldMappingItem>();

		public void MapAll() {
			foreach(var item in typeof(T).GetFields().Cast<MemberInfo>().Concat(typeof(T).GetProperties()))
				Map(item);
		}

		public int Map(string memberName) {
			var memberInfo = typeof(T).GetField(memberName) as MemberInfo ?? typeof(T).GetProperty(memberName);
			if(memberInfo == null)
				throw new InvalidOperationException($"Can't find public field or property '{memberName}'");
			return Map(memberInfo);
		}

		public int Map<TMember>(Expression<Func<T,TMember>> selector) {
			if(selector.Body.NodeType != ExpressionType.MemberAccess)
				throw new NotSupportedException();
			var m = (MemberExpression)selector.Body;
			return Map(m.Member);
		}

		public int Map(MemberInfo memberInfo) {
			var m = Expression.MakeMemberAccess(source, memberInfo);
			var column = memberInfo.SingleOrDefault<ColumnAttribute>()?.Name;
			return Map(column ?? memberInfo.Name, m.Type, DataBossDbType.ToDbType(m.Type, memberInfo), m);
		}

		public int Map<TField>(string name, Func<T, TField> selector) => 
			Map(name, typeof(TField),
				DataBossDbType.ToDbType(typeof(TField), NullAttributeProvider.Instance), 
				Expression.Invoke(Expression.Constant(selector), source));

		public int Map(string name, LambdaExpression selector) {
			if(selector.Parameters.Single().Type != typeof(T))
				throw new InvalidOperationException($"Wrong paramter type, expecte {typeof(T)}");
			var replacer = new NodeReplacementVisitor();
			replacer.AddReplacement(selector.Parameters[0], source);
			return Map(name, 
				selector.ReturnType, 
				DataBossDbType.ToDbType(selector.ReturnType, NullAttributeProvider.Instance), 
				replacer.Visit(selector.Body));
		}

		class NodeReplacementVisitor : ExpressionVisitor
		{
			readonly Dictionary<Expression, Expression> theReplacements = new Dictionary<Expression, Expression>();

			public void AddReplacement(Expression a, Expression b) => theReplacements.Add(a, b);

			public override Expression Visit(Expression node) {
				if(node == null)
					return null;

				if(theReplacements.TryGetValue(node, out var found))
					return found;
				return base.Visit(node);
			}
		}

		int Map(string name, Type type, DataBossDbType dbType, Expression selector) {
			var ordinal = mappings.Count;
			if(type.TryGetNullableTargetType(out var newTargetType)) {
				type = newTargetType;
				selector = Expression.Coalesce(selector.Box(), Expression.Constant(DBNull.Value));
			}
			mappings.Add(new FieldMappingItem {
				Name = name,
				FieldType = type,
				Selector = Expression.Assign(
					Expression.ArrayAccess(target, Expression.Constant(ordinal)),
					selector.Box()),
				DbType = dbType,
			});
			return ordinal;
		}

		public string[] GetFieldNames() => mappings.Select(x => x.Name).ToArray();
		public Type[] GetFieldTypes() => mappings.Select(x => x.FieldType).ToArray();
		public DataBossDbType[] GetDbTypes() => mappings.Select(x => x.DbType).ToArray();

		public Action<T,object[]> GetAccessor() =>
			GetAccessorExpression().Compile();

		public Expression<Action<T,object[]>> GetAccessorExpression() =>
			Expression.Lambda<Action<T,object[]>>(
				mappings.Count == 1 
				? mappings[0].Selector
				: Expression.Block(mappings.Select(x => x.Selector)), true, source, target);
	}
}