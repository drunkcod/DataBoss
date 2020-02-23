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
			var type = typeof(T);
			var fields = type.GetFields().Where(x => !x.IsStatic);
			var props = type.GetProperties().Where(x => !x.GetMethod.IsStatic);
			foreach (var item in fields.Cast<MemberInfo>().Concat(props))
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
			var m = CoerceToDbType(Expression.MakeMemberAccess(source, memberInfo));
			var column = memberInfo.SingleOrDefault<ColumnAttribute>()?.Name;
			return Map(column ?? memberInfo.Name, m.Type, DataBossDbType.ToDataBossDbType(m.Type, memberInfo), m);
		}

		public int Map<TField>(string name, Func<T, TField> selector) =>
			Map(name, selector, NullAttributeProvider.Instance);

		public int Map<TField>(string name, Func<T, TField> selector, params Attribute[] attributes) =>
			Map(name, selector, new SimpleAttributeProvider(attributes));

		int Map<TField>(string name, Func<T, TField> selector, ICustomAttributeProvider attributes) {
			var get = CoerceToDbType(Expression.Invoke(Expression.Constant(selector), source));
			var dbType = DataBossDbType.ToDataBossDbType(get.Type, attributes);
			return Map(name, get.Type, dbType, get);
		}

		class SimpleAttributeProvider : ICustomAttributeProvider
		{
			readonly Attribute[] attributes;

			public SimpleAttributeProvider(Attribute[] attributes) {
				this.attributes = attributes;
			}

			public object[] GetCustomAttributes(bool inherit) => attributes;

			public object[] GetCustomAttributes(Type attributeType, bool inherit) =>
				attributes.Where(x => attributeType.IsAssignableFrom(x.GetType())).ToArray();

			public bool IsDefined(Type attributeType, bool inherit) => attributes.Any(x => attributeType.IsAssignableFrom(x.GetType()));
		}

		static Expression CoerceToDbType(Expression get) => 
			IsIdOf(get.Type) ? Expression.Convert(get, typeof(int)) : get;

		static bool IsIdOf(Type type) => type.IsGenericType && type.GetGenericTypeDefinition() == typeof(IdOf<>);

		public int Map(string name, LambdaExpression selector) {
			if(selector.Parameters.Count != 1)
				throw new InvalidOperationException($"{nameof(LambdaExpression)} must have exactly one parameter for field '{name}'");
			if(selector.Parameters[0].Type != typeof(T))
				throw new InvalidOperationException($"Wrong paramter type, expected {typeof(T)}");
			var replacer = new NodeReplacementVisitor();
			replacer.AddReplacement(selector.Parameters[0], source);
			return Map(name, 
				selector.ReturnType, 
				DataBossDbType.ToDataBossDbType(selector.ReturnType, NullAttributeProvider.Instance), 
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
					dbType.Coerce(selector).Box()),
				DbType = dbType,
			});
			return ordinal;
		}

		public string[] GetFieldNames() => MissingLinq.ConvertAll(mappings, x => x.Name);
		public Type[] GetFieldTypes() => MissingLinq.ConvertAll(mappings, x => x.FieldType);
		public DataBossDbType[] GetDbTypes() => MissingLinq.ConvertAll(mappings, x => x.DbType);

		public Action<T,object[]> GetAccessor() =>
			GetAccessorExpression().Compile();

		public Expression<Action<T,object[]>> GetAccessorExpression() =>
			Expression.Lambda<Action<T,object[]>>(
				mappings.Count == 1 
				? mappings[0].Selector
				: Expression.Block(mappings.Select(x => x.Selector)), true, source, target);
	}
}