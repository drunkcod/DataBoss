using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Reflection;
using System.Text;
using DataBoss.Schema;

namespace DataBoss
{
	public class DataBossScripter
	{
		class DataBossTableColumn : ICustomAttributeProvider
		{
			readonly ICustomAttributeProvider attributes;

			public readonly Type ColumnType;
			public readonly string Name;

			public DataBossTableColumn(Type columnType, ICustomAttributeProvider attributes, string name) {
				this.ColumnType = columnType;
				this.attributes = attributes;
				this.Name = name;
			}

			public object[] GetCustomAttributes(Type attributeType, bool inherit) {
				return attributes.GetCustomAttributes(attributeType, inherit);
			}
			public object[] GetCustomAttributes(bool inherit){
				return attributes.GetCustomAttributes(inherit);
			}

			public bool IsDefined(Type attributeType, bool inherit) {
				return attributes.IsDefined(attributeType, inherit);
			}
		}

		class DataBossTable
		{
			readonly Type tableType;

			public static DataBossTable From(Type tableType) {
				var tableAttribute = tableType.Single<TableAttribute>();
				return new DataBossTable(tableType, tableAttribute.Name);
			}

			DataBossTable(Type tableType, string name) {
				this.tableType = tableType;
				this.Name = name;
			}

			public readonly string Name;

			public IEnumerable<DataBossTableColumn> GetColumns() {
				return tableType.GetFields()
					.Select(field => new {
						field,
						column = field.SingleOrDefault<ColumnAttribute>()
					}).Where(x => x.column != null)
					.OrderBy(x => x.column.Order)
					.Select(x => new DataBossTableColumn(x.field.FieldType, x.field, x.column.Name ?? x.field.Name));
			}
		}

		public string CreateMissing(Type tableType) {
			var table = DataBossTable.From(tableType);

			var result = new StringBuilder();
			result.AppendFormat("if object_id('{0}', 'U') is null begin", table.Name)
				.AppendLine();
			ScriptTable(table, result)
				.AppendLine();
			return ScriptConstraints(table, result)
				.AppendLine()
				.AppendLine("end")
				.ToString();
		}

		public string ScriptTable(Type tableType) {
			return ScriptTable(DataBossTable.From(tableType), new StringBuilder())
				.ToString();
		}

		StringBuilder ScriptTable(DataBossTable table, StringBuilder result) {
			result.AppendFormat("create table [{0}](" , table.Name);
			
			table.GetColumns().ForEach(x => ScriptColumn(result, x));

			result.AppendLine();
			return result.Append(')');
		}

		StringBuilder ScriptColumn(StringBuilder result, DataBossTableColumn column) {
			result.AppendLine();
			return result.AppendFormat("\t[{0}] {1},", column.Name, ToDbType(column.ColumnType, column));
		}

		public string ScriptConstraints(Type tableType) {
			var result = new StringBuilder();
			ScriptConstraints(DataBossTable.From(tableType), result);
			return result.ToString();
		}

		StringBuilder ScriptConstraints(DataBossTable table, StringBuilder result) {
			var columns = table.GetColumns().ToList();

			var clustered = columns.Where(x => x.Any<ClusteredAttribute>())
				.Select(x => x.Name)
				.ToList();
			if(clustered.Count > 0)
				result.AppendFormat("create clustered index IX_{0}_{1} on [{0}]({2})",
					table.Name,
					string.Join("_", clustered),
					string.Join(",", clustered)
				).AppendLine();

			var keys = columns.Where(x => x.Any<KeyAttribute>())
				.Select(x => x.Name)
				.ToList();
			if(keys.Count > 0) {
				result
					.AppendFormat(result.Length == 0 ? string.Empty : Environment.NewLine)
					.AppendFormat("alter table [{0}]", table.Name)
					.AppendLine()
					.AppendFormat("add constraint PK_{0} primary key(", table.Name)
					.Append(string.Join(",", keys))
					.Append(")");
			}
			return result;
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