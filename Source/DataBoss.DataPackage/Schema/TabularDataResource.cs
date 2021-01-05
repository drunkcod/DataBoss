using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text.RegularExpressions;
using DataBoss.Data;
using DataBoss.Linq;

namespace DataBoss.DataPackage
{
	public class TabularDataResource
	{
		readonly Func<IDataReader> getData;
		public readonly string Name;
		public readonly TabularDataSchema Schema;
		public string Delimiter;

		TabularDataResource(string name, TabularDataSchema schema, Func<IDataReader> getData) {
			if(!Regex.IsMatch(name, @"^[a-z0-9-._]+$"))
				throw new NotSupportedException($"name MUST consist only of lowercase alphanumeric characters plus '.', '-' and '_' was '{name}'");
			this.Name = name;
			this.Schema = schema;
			this.getData = getData;
		}

		public static TabularDataResource From(DataPackageResourceDescription desc, Func<IDataReader> getData) =>
			new TabularDataResource(desc.Name, desc.Schema, getData) { Delimiter = desc.Dialect?.Delimiter };

		public int GetOrdinal(string name) => Schema.Fields.FindIndex(x => x.Name == name);

		public IDataReader Read() {
			var reader = getData();
			if(Schema.Fields == null)
				Schema.Fields = GetFieldInfo(reader);
			return reader;
		}

		public IEnumerable<T> Read<T>() =>
			ObjectReader.Read<T>(Read());

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
				TabularDataSchemaFieldConstraints constraints = null;
				if (schema.TryGetValue(reader.GetName(i), out var found) && !found.AllowDBNull)
					constraints = new TabularDataSchemaFieldConstraints { IsRequired = true };

				var (type, format) = ToTableSchemaType(reader.GetFieldType(i));
				var field = new TabularDataSchemaFieldDescription(
					reader.GetName(i),
					type, format,
					constraints);

				r.Add(field);
			}
			return r;
		}

		static (string Type, string Format) ToTableSchemaType(Type type) {

			switch(Type.GetTypeCode(type)) {
				case TypeCode.Boolean: return ("boolean", null);
				case TypeCode.DateTime: return ("datetime", null);
				case TypeCode.Decimal:
				case TypeCode.Single:
				case TypeCode.Double: return ("number", null);
				case TypeCode.Byte:
				case TypeCode.Int16:
				case TypeCode.Int32:
				case TypeCode.Int64: return ("integer", null);
				case TypeCode.String: return ("string", null);
			}

			switch (type.FullName) {
				default:
					return (type.SingleOrDefault<FieldAttribute>()?.SchemaType ?? throw new NotSupportedException($"Can't map {type}"), null);
				case "System.TimeSpan": return ("time", null);
				case "System.Byte[]": return ("string", "binary");
			}
		}
	}
}
