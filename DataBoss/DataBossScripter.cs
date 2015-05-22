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
			var tableAttribute = tableType.Single<TableAttribute>();
			result.AppendFormat("create table [{0}](" , tableAttribute.Name);
			
			tableType.GetFields()
				.Select(field => new {
					field,
					column = field.SingleOrDefault<ColumnAttribute>()
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
			result.AppendFormat("\t[{0}] {1},", field.Name, ToDbType(field.FieldType, field));
		}

		public static string ToDbType(Type type, ICustomAttributeProvider attributes) {
			var canBeNull = !type.IsValueType && !attributes.Any<RequiredAttribute>();
			return MapType(type, attributes, ref canBeNull) + (canBeNull ? string.Empty : " not null");
		}

		private static string MapType(Type type, ICustomAttributeProvider attributes, ref bool canBeNull) {
			switch (type.FullName) {
				case "System.Int64":
					return "bigint";
				case "System.String":
					var maxLength = attributes.SingleOrDefault<MaxLengthAttribute>();
					return string.Format("varchar({0})", maxLength == null ? "max" : maxLength.Length.ToString());
				case "System.DateTime":
					return "datetime";
				default:
					if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof (Nullable<>)) {
						canBeNull = true;
						return MapType(type.GenericTypeArguments[0], attributes, ref canBeNull);
					}
					throw new NotSupportedException("Don't know how to map " + type.FullName + " to a db type");
			}
		}
	}
}