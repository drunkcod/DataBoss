using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Linq;
using System.Text.RegularExpressions;
using DataBoss.Data;
using DataBoss.DataPackage.Schema;
using DataBoss.Linq;

namespace DataBoss.DataPackage
{
	public class TabularDataResource
	{
		readonly DataPackageResourceDescription description;
		readonly Func<IDataReader> getData;
		ResourcePath resourcePath;

		public string Name => description.Name;
		public ResourcePath ResourcePath {
			get => resourcePath.IsEmpty ? description.Path : resourcePath;
			set => resourcePath = value;
		}
		public TabularDataSchema Schema => description.Schema;
		public readonly string Format;

		protected TabularDataResource(DataPackageResourceDescription description, Func<IDataReader> getData, string format) {
			if(!Regex.IsMatch(description.Name, @"^[a-z0-9-._]+$"))
				throw new NotSupportedException($"name MUST consist only of lowercase alphanumeric characters plus '.', '-' and '_' was '{description.Name}'");
			this.description = description;
			this.getData = getData;
			this.Format = format;
		}

		public static TabularDataResource From(DataPackageResourceDescription desc, Func<IDataReader> getData) {
			if (desc.Format == "csv" || (desc.Format == null && (desc.Path.All(x => x.EndsWith(".csv")))))
				return new CsvDataResource(desc, getData) { Delimiter = desc.Dialect?.Delimiter };
			return new TabularDataResource(desc, getData, desc.Format);
		}

		public DataPackageResourceDescription GetDescription() => GetDescription(null);
		public DataPackageResourceDescription GetDescription(CultureInfo culture) {
			if (Schema.Fields == null)
				throw new InvalidOperationException("Field information missing. Did you forget to call Read on new resource?");
			var desc = new DataPackageResourceDescription {
				Name = Name,
				Format = Format,
				Path = ResourcePath,
				Schema = new TabularDataSchema {
					Fields = new List<TabularDataSchemaFieldDescription>(Schema.Fields),
					PrimaryKey = NullIfEmpty(Schema.PrimaryKey),
					ForeignKeys = NullIfEmpty(Schema.ForeignKeys),
				},
			};

			UpdateDescription(desc);

			var outputFields = desc.Schema.Fields;
			var decimalCharOverride = culture?.NumberFormat.NumberDecimalSeparator;
			if (decimalCharOverride != null) {
				for (var i = 0; i != outputFields.Count; ++i) {
					var field = outputFields[i];
					if (field.IsNumber() && field.DecimalChar != decimalCharOverride)
						outputFields[i] = field.WithDecimalChar(decimalCharOverride);
				}
			}

			return desc;
		}

		protected virtual void UpdateDescription(DataPackageResourceDescription description) { }

		static List<T> NullIfEmpty<T>(List<T> values) => (values == null || values.Count == 0) ? null : values;

		public int GetOrdinal(string name) => Schema.Fields.FindIndex(x => x.Name == name);

		public IDataReader Read() {
			var reader = getData();
			if(Schema.Fields == null)
				Schema.Fields = GetFieldInfo(reader);
			return reader;
		}

		public IEnumerable<T> Read<T>() => 
			ObjectReader.Read<T>(Read(), CustomConverters);

		static readonly ConverterCollection CustomConverters = new() {
			new Func<string, char>(StringToChar),
		};

		static char StringToChar(string input) =>
			input.Length == 1 ? input[0] : throw new InvalidConversionException($"expected string of length 1, was '{input}'", typeof(char));

		public TabularDataResource Where(Func<IDataRecord, bool> predicate) =>
			Rebind(Name, Schema.Clone(), () => getData().Where(predicate));

		public TabularDataResource Transform(Action<DataReaderTransform> defineTransform) => 
			Rebind(Name, SchemaWithSameKeys(), () => {
				var data = new DataReaderTransform(getData());
				defineTransform(data);
				return data;
			});

		public TabularDataResource WithData(Func<IDataReader> newData) =>
			Rebind(Name, SchemaWithSameKeys(), newData);

		public TabularDataResource WithName(string name) =>
			Rebind(name, SchemaWithSameKeys(), getData);

		TabularDataSchema SchemaWithSameKeys() =>
			new TabularDataSchema {
				PrimaryKey = Schema.PrimaryKey?.ToList(),
				ForeignKeys = Schema.ForeignKeys?.ToList()
			};

		public TabularDataResource Rebind(string name, Func<IDataReader> getData) =>
			Rebind(name, new TabularDataSchema(), getData);

		protected virtual TabularDataResource Rebind(string name, TabularDataSchema schema, Func<IDataReader> getData) =>
			new(new DataPackageResourceDescription {
				Name = name,
				Schema = schema,
				Path = description.Path,
			}, getData, Format) { ResourcePath = ResourcePath };

		static List<TabularDataSchemaFieldDescription> GetFieldInfo(IDataReader reader) {
			var r = new List<TabularDataSchemaFieldDescription>(reader.FieldCount);
			var schema = reader
				.GetDataReaderSchemaTable()
				.ToDictionary(x => x.Ordinal, x => x);
			
			for (var i = 0; i != reader.FieldCount; ++i) {
				TabularDataSchemaFieldConstraints? constraints = null;
				if (schema.TryGetValue(i, out var found)) {
					if(!found.AllowDBNull)
						constraints = new TabularDataSchemaFieldConstraints(true, constraints?.MaxLength);
					if ((found.ColumnType == typeof(string) || found.ColumnType == typeof(char)) && found.ColumnSize != int.MaxValue)
						constraints = new TabularDataSchemaFieldConstraints(constraints?.IsRequired ?? false, found.ColumnSize);
				}

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

				case TypeCode.Single:
				case TypeCode.Double: 
				case TypeCode.Decimal: return ("number", null);

				case TypeCode.Byte:
				case TypeCode.Int16:
				case TypeCode.Int32:
				case TypeCode.Int64: return ("integer", null);
				case TypeCode.Char:
				case TypeCode.String: return ("string", null);
			}

			return type.FullName switch {
				"System.TimeSpan" => ("time", null),
				"System.Byte[]" => ("string", "binary"),
				"System.Guid" => ("string", "uuid"),
				_ => (type.SingleOrDefault<FieldAttribute>()?.SchemaType ?? throw new NotSupportedException($"Can't map {type}"), null),
			};
		}
	}
}
