using DataBoss.Linq;
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace DataBoss.Data
{
	public class FieldMapping
	{
		struct FieldMappingItem
		{
			public string Name;
			public Type FieldType;
			public Expression Selector;
			public DataBossDbType DbType;
		}

		class NodeReplacementVisitor : ExpressionVisitor
		{
			readonly Dictionary<Expression, Expression> theReplacements = new Dictionary<Expression, Expression>();

			public void AddReplacement(Expression a, Expression b) => theReplacements.Add(a, b);

			public override Expression Visit(Expression node) {
				if (node == null)
					return null;

				if (theReplacements.TryGetValue(node, out var found))
					return found;
				return base.Visit(node);
			}
		}

		protected readonly ParameterExpression Source;
		protected ParameterExpression Target;
		protected Type SourceType => Source.Type;

		readonly List<FieldMappingItem> mappings;

		public FieldMapping(Type sourceType) {
			this.Source = Expression.Parameter(sourceType, "source");
			this.Target = Expression.Parameter(typeof(object[]), "target");
			this.mappings = new List<FieldMappingItem>();

		}

		public string[] GetFieldNames() => MissingLinq.ConvertAll(mappings, x => x.Name);
		public Type[] GetFieldTypes() => MissingLinq.ConvertAll(mappings, x => x.FieldType);
		public DataBossDbType[] GetDbTypes() => MissingLinq.ConvertAll(mappings, x => x.DbType);

		public void MapAll() {
			var fields = SourceType.GetFields().Where(x => !x.IsStatic);
			var props = SourceType.GetProperties().Where(x => !x.GetMethod.IsStatic);
			foreach (var item in fields.Cast<MemberInfo>().Concat(props))
				Map(item);
		}

		public int Map(string memberName) {
			var memberInfo = SourceType.GetField(memberName) as MemberInfo ?? SourceType.GetProperty(memberName);
			if (memberInfo == null)
				throw new InvalidOperationException($"Can't find public field or property '{memberName}'");
			return Map(memberInfo);
		}

		public int Map(MemberInfo memberInfo) {
			var m = CoerceToDbType(Expression.MakeMemberAccess(Source, memberInfo));
			var column = memberInfo.SingleOrDefault<ColumnAttribute>()?.Name;
			return Map(column ?? memberInfo.Name, m.Type, DataBossDbType.From(m.Type, memberInfo), m);
		}

		protected int Map(string name, Type type, DataBossDbType dbType, Expression selector) {
			if (type.TryGetNullableTargetType(out var newTargetType)) {
				type = newTargetType;
				var xarg = Expression.Parameter(selector.Type, "x");
				selector = Expression.Coalesce(selector, Expression.Constant(DBNull.Value, typeof(object)), 
					Expression.Lambda(Expression.Convert(xarg, typeof(object)), xarg));
			}
			var ordinal = mappings.Count;
			mappings.Add(new FieldMappingItem {
				Name = name,
				FieldType = type,
				Selector = Coerce(dbType, selector),
				DbType = dbType,
			});
			return ordinal;
		}

		public int Map(string name, LambdaExpression selector) {
			if (selector.Parameters.Count != 1)
				throw new InvalidOperationException($"{nameof(LambdaExpression)} must have exactly one parameter for field '{name}'");
			if (selector.Parameters[0].Type != SourceType)
				throw new InvalidOperationException($"Wrong paramter type, expected {SourceType}");
			var replacer = new NodeReplacementVisitor();
			replacer.AddReplacement(selector.Parameters[0], Source);
			return Map(name,
				selector.ReturnType,
				DataBossDbType.From(selector.ReturnType, NullAttributeProvider.Instance),
				replacer.Visit(selector.Body));
		}

		public Expression GetSelector(int ordinal) =>
			mappings[ordinal].Selector;

		public Expression GetAccessorBody() {
			if(mappings.Count == 1)
				return AssignTarget(0, mappings[0].Selector);
			var assignments = new Expression[mappings.Count];
			for (var i = 0; i != mappings.Count; ++i)
				assignments[i] = AssignTarget(i, mappings[i].Selector);
			return Expression.Block(assignments);
		}

		Expression AssignTarget(int ordinal, Expression selector) =>
			Expression.Assign(
				Expression.ArrayAccess(Target, Expression.Constant(ordinal)),
				selector.Box());

		static Expression Coerce(DataBossDbType dbType, Expression expr) => 
			dbType.IsRowVersion ? Expression.Convert(expr, typeof(byte[])) : expr;

		protected static Expression CoerceToDbType(Expression get) =>
			TryGetConvertible(get.Type, out var dbType) ? Expression.Convert(get, dbType) : get;

		static bool TryGetConvertible(Type type, out Type dbType) {
			var found = type.SingleOrDefault<DbTypeAttribute>();

			if (found != null) {
				dbType = found.Type;
				return true;
			}

			dbType = null;
			return false;
		}
	}

	public class FieldMapping<T> : FieldMapping
	{
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

		public FieldMapping() : base(typeof(T)) { }

		public int Map<TMember>(Expression<Func<T,TMember>> selector) {
			if(selector.Body.NodeType != ExpressionType.MemberAccess)
				throw new NotSupportedException();
			var m = (MemberExpression)selector.Body;
			return Map(m.Member);
		}

		public int Map<TField>(string name, Func<T, TField> selector) =>
			Map(name, selector, NullAttributeProvider.Instance);

		public int Map<TField>(string name, Func<T, TField> selector, params Attribute[] attributes) =>
			Map(name, selector, new SimpleAttributeProvider(attributes));

		int Map<TField>(string name, Func<T, TField> selector, ICustomAttributeProvider attributes) {
			var get = CoerceToDbType(Expression.Invoke(Expression.Constant(selector), Source));
			var dbType = DataBossDbType.From(get.Type, attributes);
			return Map(name, get.Type, dbType, get);
		}

		public Action<T,object[]> GetAccessor() =>
			GetAccessorExpression().Compile();

		public Expression<Action<T,object[]>> GetAccessorExpression() =>
			Expression.Lambda<Action<T,object[]>>(GetAccessorBody(), true, Source, Target);
	}
}