using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text.RegularExpressions;
using DataBoss.Data;

namespace DataBoss.DataPackage
{
	public class TabularDataResource
	{
		readonly Func<IDataReader> getData;
		public readonly string Name;
		public readonly TabularDataSchema Schema;

		public TabularDataResource(string name, TabularDataSchema schema, Func<IDataReader> getData) {
			if(!Regex.IsMatch(name, @"^[a-z0-9-._]+$"))
				throw new NotSupportedException($"name MUST consist only of lowercase alphanumeric characters plus '.', '-' and '_' was '{name}'");
			this.Name = name;
			this.Schema = schema;
			this.getData = getData;
		}

		public int GetOrdinal(string name) => Schema.Fields.FindIndex(x => x.Name == name);

		public IDataReader Read() {
			var reader = getData();
			if(Schema.Fields == null)
				Schema.Fields = GetFieldInfo(reader);
			return reader;
		}

		public IEnumerable<T> Read<T>() => 
			ObjectReader.For(Read()).Read<T>();

		public TabularDataResource Where(Func<IDataRecord, bool> predicate) =>
			new TabularDataResource(Name, Schema, () => new WhereDataReader(getData(), predicate));

		public TabularDataResource Transform(Action<DataReaderTransform> defineTransform) {
			var schema = new TabularDataSchema();
			schema.PrimaryKey = Schema.PrimaryKey?.ToList();
			schema.ForeignKeys = Schema.ForeignKeys?.ToList();
			return new TabularDataResource(Name, schema, () => {
				var data = new DataReaderTransform(getData());
				defineTransform(data);
				return data;
			});
		}

		static List<TabularDataSchemaFieldDescription> GetFieldInfo(IDataReader reader) {
			var r = new List<TabularDataSchemaFieldDescription>(reader.FieldCount);
			var schema = ObjectReader.For(reader.GetSchemaTable().CreateDataReader())
				.Read<DataReaderSchemaRow>()
				.ToDictionary(x => x.ColumnName, x => x);
			
			for (var i = 0; i != reader.FieldCount; ++i) {
				var field = new TabularDataSchemaFieldDescription {
					Name = reader.GetName(i),
					Type = ToTableSchemaType(reader.GetFieldType(i)),
				};
				if(schema.TryGetValue(reader.GetName(i), out var found) && !found.AllowDBNull)
					field.Constraints = new TabularDataSchemaFieldConstraints { IsRequired = true };
				r.Add(field);
			}
			return r;
		}

		static string ToTableSchemaType(Type type) {
			switch (type.FullName) {
				default:
					throw new NotSupportedException($"Can't map {type}");
				case "System.Boolean": return "boolean";
				case "System.DateTime": return "datetime";
				case "System.Decimal":
				case "System.Single":
				case "System.Double": return "number";
				case "System.Byte":
				case "System.Int16":
				case "System.Int32": 
				case "System.Int64": return "integer";
				case "System.String": return "string";
			}
		}
	}
}
