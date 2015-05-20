using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Reflection;
using System.Text;

namespace DataBoss
{
	public class DataBossScripter
	{
		public string Script(Type tableType) {
			var result = new StringBuilder();

			var tableAttribute = tableType.GetCustomAttributes(typeof(TableAttribute), true).Cast<TableAttribute>().Single();
			result.AppendFormat("create table [{0}](" , tableAttribute.Name);
			
			tableType.GetFields()
				.Select(field => new {
					field,
					column = field.GetCustomAttributes(typeof(ColumnAttribute), true).Cast<ColumnAttribute>().SingleOrDefault()
				}).Where(x => x.column != null)
				.OrderBy(x => x.column.Order)
				.ToList()
				.ForEach(x => ScriptColumn(result, x.field));
				
			result.AppendLine();
			result.Append(')');
			return result.ToString();
		}

		void ScriptColumn(StringBuilder result, FieldInfo field) {
			result.AppendLine();
			result.AppendFormat("\t[{0}] {1},", field.Name, ToDbType(field, field.FieldType));
		}

		public static string ToDbType(MemberInfo field, Type memberType) {
			return ToDbType(field, 
				memberType, !memberType.IsValueType && field.GetCustomAttributes(typeof(RequiredAttribute), true).Length == 0);
		}

		static string ToDbType(MemberInfo member, Type type, bool canBeNull) {
			return MapType(member, type, ref canBeNull) + (canBeNull ? string.Empty : " not null");
		}

		private static string MapType(MemberInfo member, Type type, ref bool canBeNull) {
			switch (type.FullName) {
				case "System.Int64":
					return "bigint";
				case "System.String":
					var maxLength =
						member.GetCustomAttributes(typeof (MaxLengthAttribute), true).Cast<MaxLengthAttribute>().SingleOrDefault();
					return string.Format("varchar({0})", maxLength == null ? "max" : maxLength.Length.ToString());
				case "System.DateTime":
					return "datetime";
				default:
					if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof (Nullable<>)) {
						canBeNull = true;
						return MapType(member, type.GenericTypeArguments[0], ref canBeNull);
					}
					throw new NotSupportedException("Don't know how to map " + type.FullName + " to a db type");
			}
		}
	}
}