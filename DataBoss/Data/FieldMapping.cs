﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace DataBoss.Data
{
	public class FieldMapping<T>
	{
		readonly ParameterExpression source = Expression.Parameter(typeof(T), "source");
		KeyValuePair<string, Expression>[] selectors = new KeyValuePair<string, Expression>[0];
		Type[] memberTypes = new Type[0];

		public int Map(string memberName) {
			var memberInfo = (typeof(T).GetField(memberName) as MemberInfo) ?? typeof(T).GetProperty(memberName);
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

		public int Map(string name, Func<T, object> selector) => Map(name, typeof(object),
			Expression.Invoke(Expression.Constant(selector), source));

		int Map(string name, Type type, Expression selector) {
			var ordinal = selectors.Length;
			Array.Resize(ref selectors, ordinal + 1);
			Array.Resize(ref memberTypes, ordinal + 1);

			selectors[ordinal] = new KeyValuePair<string, Expression>(name, selector);
			memberTypes[ordinal] = type;

			return ordinal;
		}

		public string[] GetFieldNames() => Array.ConvertAll(selectors, x => x.Key);
		public Type[] GetFieldTypes() => memberTypes;

		public Action<T,object[]> GetAccessor() {
			var target = Expression.Parameter(typeof(object[]), "target");
			var body = Enumerable.Range(0, selectors.Length)
				.Select(x => Expression.Assign(
					Expression.ArrayAccess(target, Expression.Constant(x)),
					selectors[x].Value));
			return Expression.Lambda<Action<T,object[]>>(
				Expression.Block(body), true, source, target).Compile();
		}
	}
}