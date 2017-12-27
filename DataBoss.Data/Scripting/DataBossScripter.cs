using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data;
using System.Linq;
using System.Reflection;
using System.Text;
using DataBoss.Linq;

namespace DataBoss.Data.Scripting
{
	[AttributeUsage(AttributeTargets.Field | AttributeTargets.Property)]
	public class AnsiStringAttribute : Attribute
	{ }

	public class DataBossScripter
	{
		class DataBossTableColumn : ICustomAttributeProvider
		{
			readonly ICustomAttributeProvider attributes;

			public readonly string Name;
			public readonly DataBossDbType ColumnType;

			public DataBossTableColumn(DataBossDbType columnType, ICustomAttributeProvider attributes, string name) {
				this.ColumnType = columnType;
				this.attributes = attributes;
				this.Name = name;
			}

			public object[] GetCustomAttributes(Type attributeType, bool inherit) =>
				attributes.GetCustomAttributes(attributeType, inherit);

			public object[] GetCustomAttributes(bool inherit) =>
				attributes.GetCustomAttributes(inherit);

			public bool IsDefined(Type attributeType, bool inherit) =>
				attributes.IsDefined(attributeType, inherit);
		}

		class DataBossTable
		{
			readonly List<DataBossTableColumn> columns;

			public static DataBossTable From(Type tableType) {
				var tableAttribute = tableType.Single<TableAttribute>();
				return new DataBossTable(tableAttribute.Name, tableAttribute.Schema, 
					tableType.GetFields()
					.Select(field => new {
						field,
						column = field.SingleOrDefault<ColumnAttribute>()
					}).Where(x => x.column != null)
					.Select(x => new {
						Order = x.column.Order == -1 ? int.MaxValue : x.column.Order,
						x.field.FieldType,
						Field = x.field,
						Name = x.column.Name ?? x.field.Name,
					})
					.OrderBy(x => x.Order)
					.Select(x => new DataBossTableColumn(DataBossDbType.ToDbType(x.FieldType, x.Field), x.Field, x.Name)));
			}

			public DataBossTable(string name, string schema, IEnumerable<DataBossTableColumn> columns) {
				this.Name = name;
				this.Schema = schema;
				this.columns = columns.ToList();
			}

			public readonly string Name;
			public readonly string Schema;

			public string FullName => string.IsNullOrEmpty(Schema) ? $"[{Name}]" : $"[{Schema}].[{Name}]";
			public IReadOnlyList<DataBossTableColumn> Columns => columns.AsReadOnly();
		}

		public string CreateMissing(Type tableType) {
			var table = DataBossTable.From(tableType);

			var result = new StringBuilder();
			result.AppendFormat("if object_id('{0}', 'U') is null begin", table.FullName)
				.AppendLine();
			ScriptTable(table, result)
				.AppendLine();
			return ScriptConstraints(table, result)
				.AppendLine()
				.AppendLine("end")
				.ToString();
		}

		public string ScriptTable(Type tableType) =>
			ScriptTable(DataBossTable.From(tableType), new StringBuilder()).ToString();

		public string ScriptTable(string name, IDataReader reader) {
			var columns = new List<DataBossTableColumn>();
			var schema = reader.GetSchemaTable();
			var isNullable = schema.Columns[DataReaderSchemaColumns.AllowDBNull];
			var columnSize = schema.Columns[DataReaderSchemaColumns.ColumnSize];
			for(var i = 0; i != reader.FieldCount; ++i) {
				var r = schema.Rows[i];
				columns.Add(new DataBossTableColumn(new DataBossDbType(
					reader.GetDataTypeName(i),
					(columnSize == null  || r[columnSize] is DBNull) ? new int?(): (int)r[columnSize],
					(bool)r[isNullable]), 
					NullAttributeProvider.Instance, 
					reader.GetName(i)));
			}

			var table = new DataBossTable(name, string.Empty, columns);
			return ScriptTable(table, new StringBuilder()).ToString();
		}

		StringBuilder ScriptTable(DataBossTable table, StringBuilder result) {
			result.Append("create table ");
			AppendTableName(result, table)
				.Append("(");
			
			var sep = "\r\n\t";
			foreach(var item in table.Columns) {
				ScriptColumn(result.Append(sep), item);
				sep = ",\r\n\t";
			}

			result.AppendLine();
			return result.Append(')');
		}

		StringBuilder ScriptColumn(StringBuilder result, DataBossTableColumn column) =>
			result.AppendFormat("[{0}] {1}", column.Name, column.ColumnType);

		public string ScriptConstraints(Type tableType) {
			var result = new StringBuilder();
			ScriptConstraints(DataBossTable.From(tableType), result);
			return result.ToString();
		}

		StringBuilder ScriptConstraints(DataBossTable table, StringBuilder result) {
			var clustered = table.Columns.Where(x => x.Any<ClusteredAttribute>())
				.Select(x => x.Name)
				.ToList();
			if(clustered.Count > 0)
				AppendTableName(result.AppendFormat("create clustered index IX_{0}_{1} on ", table.Name, string.Join("_", clustered)), table)
				.AppendFormat("({0})", string.Join(",", clustered))
				.AppendLine();

			var keys = table.Columns.Where(x => x.Any<KeyAttribute>())
				.Select(x => x.Name)
				.ToList();
			if(keys.Count > 0) {
				result.AppendFormat(result.Length == 0 ? string.Empty : Environment.NewLine);
				AppendTableName(result.AppendFormat("alter table "), table)
					.AppendLine()
					.AppendFormat("add constraint PK_{0} primary key(", table.Name)
					.Append(string.Join(",", keys))
					.Append(")");
			}
			return result;
		}

		public string Select(Type rowType, Type tableType) {
			var table = DataBossTable.From(tableType);
			var result = new StringBuilder()
				.AppendFormat("select {0} from ", string.Join(", ", rowType.GetFields().Where(x => !x.IsInitOnly).Select(x => x.Name)));
			return AppendTableName(result, table).ToString();
		}

		static StringBuilder AppendTableName(StringBuilder target, DataBossTable table) {
			if(!string.IsNullOrEmpty(table.Schema))
				target.AppendFormat("[{0}].", table.Schema);
			return target.AppendFormat("[{0}]" , table.Name);
		}
	}
}