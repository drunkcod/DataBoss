using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace DataBoss.Data
{
	public class FieldMapping<T>
	{
		readonly ParameterExpression source = Expression.Parameter(typeof(T), "source");
		readonly ParameterExpression target = Expression.Parameter(typeof(object[]), "target");

		KeyValuePair<KeyValuePair<string,Type>, Expression>[] selectors = new KeyValuePair<KeyValuePair<string,Type>, Expression>[0];

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
			return Map(memberInfo.Name, m.Type, Expression.Convert(m, typeof(object)));
		}

		public int Map(string name, Func<T, object> selector) => 
			Map(name, typeof(object), Expression.Invoke(Expression.Constant(selector), source));

		int Map(string name, Type type, Expression selector) {
			var ordinal = selectors.Length;
			Array.Resize(ref selectors, ordinal + 1);

			selectors[ordinal] = new KeyValuePair<KeyValuePair<string,Type>, Expression>(
					new KeyValuePair<string, Type>(name, type),
				Expression.Assign(
					Expression.ArrayAccess(target, Expression.Constant(ordinal)),
					selector));

			return ordinal;
		}

		public string[] GetFieldNames() => Array.ConvertAll(selectors, x => x.Key.Key);
		public Type[] GetFieldTypes() => Array.ConvertAll(selectors, x => x.Key.Value);

		public Action<T,object[]> GetAccessor() =>
			Expression.Lambda<Action<T,object[]>>(
				Expression.Block(selectors.Select(x => x.Value)), true, source, target).Compile();
	}
}