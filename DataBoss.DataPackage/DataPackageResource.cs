using System;
using System.Collections.Generic;
using System.Data;
using System.Text.RegularExpressions;

namespace DataBoss.DataPackage
{
	public class DataPackageResource
	{
		readonly Func<IDataReader> getData;
		public readonly string Name;
		public readonly DataPackageTabularSchema Schema;

		public DataPackageResource(string name, DataPackageTabularSchema schema, Func<IDataReader> getData) {
			if(!Regex.IsMatch(name, @"^[a-z0-9-._]+$"))
				throw new NotSupportedException($"name MUST consist only of lowercase alphanumeric characters plus '.', '-' and '_' was '{name}'");
			this.Name = name;
			this.Schema = schema;
			this.getData = getData;
		}

		public IDataReader Read() {
			var reader = getData();
			if(Schema.Fields == null)
				Schema.Fields = GetFieldInfo(reader);
			return reader;
		}

		static List<DataPackageTabularFieldDescription> GetFieldInfo(IDataReader reader) {
			var r = new List<DataPackageTabularFieldDescription>(reader.FieldCount);
			for (var i = 0; i != reader.FieldCount; ++i) {
				r.Add(new DataPackageTabularFieldDescription {
					Name = reader.GetName(i),
					Type = ToTableSchemaType(reader.GetFieldType(i)),
				});
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
				case "System.Double": return "number";
				case "System.Byte":
				case "System.Int16":
				case "System.Int32": return "integer";
				case "System.String": return "string";
			}
		}
	}


}
