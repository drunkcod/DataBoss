using DataBoss.Expressions;
using DataBoss.Linq;
using DataBoss.Linq.Expressions;
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace DataBoss.Data
{
	readonly struct FieldMappingItem
	{
		public readonly string Name;
		public readonly Type FieldType;
		public readonly Expression HasValue;
		public readonly Expression GetValue;
		public readonly Expression Selector;
		public readonly DataBossDbType DbType;

		public FieldMappingItem(string name, Type fieldType, Expression hasValue, Expression getValue, Expression selector, DataBossDbType dbType) {
			this.Name = name;
			this.FieldType = fieldType;
			this.HasValue = hasValue;
			this.GetValue = getValue;
			this.Selector = selector;
			this.DbType = dbType;
		}
	}

	public class FieldMapping
	{
		public readonly ParameterExpression Source;
		protected readonly ParameterExpression Target;
		protected Type SourceType => Source.Type;

		readonly List<FieldMappingItem> mappings;

		public FieldMapping(Type sourceType) {
			this.Source = Expression.Parameter(sourceType, "source");
			this.Target = Expression.Parameter(typeof(object[]), "target");
			this.mappings = new List<FieldMappingItem>();
		}

		public int Count => mappings.Count;

		internal FieldMappingItem this[int index] => mappings[index];

		public string GetFieldName(int i) => mappings[i].Name;
		public Type GetFieldType(int i) => mappings[i].FieldType;
		public DataBossDbType GetDbType(int i) => mappings[i].DbType;

		public void MapAll() {
			var fields = SourceType.GetFields().Where(x => !x.IsStatic);
			var props = SourceType.GetProperties().Where(x => !x.GetMethod.IsStatic);
			foreach (var item in fields.Cast<MemberInfo>().Concat(props))
				Map(item);
		}

		public int Map(string memberName) =>
			Map(SourceType.GetField(memberName) as MemberInfo 
				?? SourceType.GetProperty(memberName)
				?? throw new InvalidOperationException($"Can't find public field or property '{memberName}'"));

		public int Map(MemberInfo memberInfo) {
			var (m, attr) = CoerceToDbType(Expression.MakeMemberAccess(Source, memberInfo), memberInfo);
			var column = memberInfo.SingleOrDefault<ColumnAttribute>()?.Name;
			return Map(column ?? memberInfo.Name, m.Type, DataBossDbType.From(m.Type, attr), m);
		}

		protected int Map(string name, Type type, DataBossDbType dbType, Expression selector) {
			Expression hasValue = null;
			Expression getValue = null;
			if (type == typeof(string)) {
				hasValue = Expression.NotEqual(selector, Expression.Constant(null, type));
				getValue = selector;
			} else if (type.TryGetNullableTargetType(out var newTargetType)) {
				hasValue = Expression.Property(selector, "HasValue");
				getValue = Expression.Property(selector, "Value");
				selector = Expression.Condition(
					hasValue,
					getValue.Box(),
					Expression.Constant(DBNull.Value, typeof(object)));
				type = newTargetType;
			}

			var ordinal = mappings.Count;
			mappings.Add(new FieldMappingItem(
				name,
				type,
				hasValue,
				getValue,
				Coerce(dbType, selector),
				dbType));

			return ordinal;
		}

		public int Map(string name, LambdaExpression selector) {
			if (selector.Parameters.Count != 1)
				throw new InvalidOperationException($"{nameof(LambdaExpression)} must have exactly one parameter for field '{name}'");
			
			if (selector.Parameters[0].Type != SourceType)
				throw new InvalidOperationException($"Wrong paramter type, expected {SourceType}");
			
			var replacer = new NodeReplacementVisitor();
			replacer.Add(selector.Parameters[0], Source);
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

		protected static (Expression, ICustomAttributeProvider) CoerceToDbType(Expression get, ICustomAttributeProvider attributes) { 			
			var dbTypeAttribute = get.Type.SingleOrDefault<DbTypeAttribute>();
			return TryGetConvertible(dbTypeAttribute, out var dbType) 
			? (Expression.Convert(get, dbType), new CombinedAttributesProvider(dbTypeAttribute, attributes)) 
			: (get, attributes);
		}

		class CombinedAttributesProvider : ICustomAttributeProvider
		{
			readonly ICustomAttributeProvider first;
			readonly ICustomAttributeProvider second;

			public CombinedAttributesProvider(ICustomAttributeProvider first, ICustomAttributeProvider second) {
				this.first = first;
				this.second = second;
			}

			public object[] GetCustomAttributes(bool inherit) {
				var xs = first.GetCustomAttributes(inherit);
				var ys = second.GetCustomAttributes(inherit);
				return xs is null ? ys : xs.Concat(ys.EmptyIfNull()).ToArray();
			}

			public object[] GetCustomAttributes(Type attributeType, bool inherit) {
				var xs = first.GetCustomAttributes(attributeType, inherit);
				var ys = second.GetCustomAttributes(attributeType, inherit);
				return xs is null ? ys : xs.Concat(ys.EmptyIfNull()).ToArray();
			}

			public bool IsDefined(Type attributeType, bool inherit) =>
				first.IsDefined(attributeType, inherit) || second.IsDefined(attributeType, inherit);
		}

		static bool TryGetConvertible(DbTypeAttribute dbTypeAttr, out Type dbType) {
			if (dbTypeAttr != null) {
				dbType = dbTypeAttr.Type;
				return true;
			}

			dbType = null;
			return false;
		}

		public object[] Invoke(object obj) {
			var r = new object[Count];
			Expression.Lambda(GetAccessorBody(), Source, Target).Compile().DynamicInvoke(obj, r);
			return r;
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
			var (get, attr) = CoerceToDbType(Expression.Invoke(Expression.Constant(selector), Source), attributes);
			var dbType = DataBossDbType.From(get.Type, attr);
			return Map(name, get.Type, dbType, get);
		}

		public Action<T,object[]> GetAccessor() =>
			GetAccessorExpression().Compile();

		public Expression<Action<T,object[]>> GetAccessorExpression() =>
			Expression.Lambda<Action<T,object[]>>(GetAccessorBody(), true, Source, Target);
	}
}