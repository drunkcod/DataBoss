using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Linq.Expressions;
using System.Security.Cryptography.X509Certificates;

namespace DataBoss
{
	public class ObjectReader<T> where T : new()
	{
		public IEnumerable<T> Convert(IDataReader source) {
			var converter = GetConverter(source).Compile();

			while(source.Read()) {
				yield return converter(source);
			}
		} 

		public Expression<Func<IDataRecord, T>> GetConverter(IDataReader reader) {
			var arg0 = Expression.Parameter(typeof(IDataRecord), "x");
			var fieldMap = new Dictionary<string, int>();
			for(var i = 0; i != reader.FieldCount; ++i)
				fieldMap[reader.GetName(i)] = i;
			
			var dummy = 0;
			var fields = typeof(T)
				.GetFields()
				.Where(x => !x.IsInitOnly)
				.Where(x => fieldMap.TryGetValue(x.Name, out dummy))
				.Select(x => new { x, ordinal = dummy })
				.Select(x => Expression.Bind(x.x, Expression.Call(arg0, typeof(IDataRecord).GetMethod("Get" + x.x.FieldType.Name), Expression.Constant(x.ordinal))));
			var init = Expression.MemberInit(
				Expression.New(typeof(T).GetConstructor(Type.EmptyTypes)), fields.Cast<MemberBinding>() 
			);
			return Expression.Lambda<Func<IDataRecord, T>>(init, arg0);
		}
	}
}