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

		public void AddForeignKey(string field, DataPackageKeyReference reference) =>
			(Schema.ForeignKeys ??= new()).Add(new DataPackageForeignKey(field, reference));

		protected virtual void UpdateDescription(DataPackageResourceDescription description) { }

		static List<T> NullIfEmpty<T>(List<T> values) => (values == null || values.Count == 0) ? null : values;

		public int GetOrdinal(string name) => Schema.Fields.FindIndex(x => x.Name == name);

		public IDataReader Read() {
			var reader = getData();
			if(Schema.Fields == null)
				Schema.Fields = GetFieldInfo(reader);
			return reader;
		}

		public IEnumerable<T> Read<T>() => ObjectReader.Enumerable<T>(Read, CustomConverters);

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
			new() {
				PrimaryKey = Schema.PrimaryKey?.ToList(),
				ForeignKeys = Schema.ForeignKeys?.ToList()
			};

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
					if ((found.DataType == typeof(string) || found.DataType == typeof(char)) && found.ColumnSize != int.MaxValue)
						constraints = new TabularDataSchemaFieldConstraints(constraints?.IsRequired ?? false, found.ColumnSize);
				}

				var tableType = TableSchemaType.From(reader.GetFieldType(i));
				var field = new TabularDataSchemaFieldDescription(
					reader.GetName(i),
					tableType.TableTypeName, tableType.Format,
					constraints);

				r.Add(field);
			}
			return r;
		}
	}

	class TableSchemaType
	{
		public readonly Type Type;
		public readonly string TableTypeName;
		public readonly string Format;
		public readonly CsvTypeCode CsvTypeCode;
		public Type CsvType => CsvTypeCode switch {
			CsvTypeCode.None => null,
			CsvTypeCode.CsvInteger => typeof(CsvInteger),
			CsvTypeCode.CsvNumber => typeof(CsvNumber),
			_ => throw new InvalidOperationException($"{CsvTypeCode} not mapped to a Type."),
		};

		TableSchemaType(Type type, string typeName, string format, CsvTypeCode csvTypeCode) { 
			this.Type = type;
			this.TableTypeName = typeName;
			this.Format = format;
			this.CsvTypeCode = csvTypeCode;
		}

		static readonly TableSchemaType Boolean = new(typeof(bool), "boolean", null, CsvTypeCode.None);
		static readonly TableSchemaType Binary = new(typeof(byte[]), "string", "binary", CsvTypeCode.None);
		static readonly TableSchemaType Char = new(typeof(char), "string", null, CsvTypeCode.None);
		static readonly TableSchemaType Date = new(typeof(DateTime), "date", null, CsvTypeCode.None);
		static readonly TableSchemaType DateTime = new(typeof(DateTime), "datetime", null, CsvTypeCode.None);
		static readonly TableSchemaType Uuid = new(typeof(Guid), "string", "uuid", CsvTypeCode.None);
		static readonly TableSchemaType String = new(typeof(string), "string", null, CsvTypeCode.None);
		static readonly TableSchemaType Time = new(typeof(TimeSpan), "time", null, CsvTypeCode.None);

		static readonly TableSchemaType Single = new(typeof(float), "number", null, CsvTypeCode.CsvNumber);
		static readonly TableSchemaType Double = new(typeof(double), "number", null, CsvTypeCode.CsvNumber);
		static readonly TableSchemaType Decimal = new(typeof(decimal), "number", null, CsvTypeCode.CsvNumber);

		static readonly TableSchemaType Byte = new(typeof(byte), "integer", null, CsvTypeCode.CsvInteger);
		static readonly TableSchemaType Int16 = new(typeof(short), "integer", null, CsvTypeCode.CsvInteger);
		static readonly TableSchemaType Int32 = new(typeof(int), "integer", null, CsvTypeCode.CsvInteger);
		static readonly TableSchemaType Int64 = new(typeof(long), "integer", null, CsvTypeCode.CsvInteger);

		public static TableSchemaType From(Type type)=>
			Type.GetTypeCode(type) switch {
				TypeCode.Boolean => Boolean,
				TypeCode.DateTime => DateTime,
				TypeCode.Single => Single,
				TypeCode.Double => Double,
				TypeCode.Decimal => Decimal,
				TypeCode.Byte => Byte,
				TypeCode.Int16 => Int16,
				TypeCode.Int32 => Int32,
				TypeCode.Int64 => Int64,
				TypeCode.Char => Char,
				TypeCode.String => String,
				_ => type.FullName switch {
					"System.TimeSpan" => Time,
					"System.Byte[]" => Binary,
					"System.Guid" => Uuid,
					_ => new(type, type.SingleOrDefault<FieldAttribute>()?.SchemaType ?? throw new NotSupportedException($"Can't map {type}"), null, CsvTypeCode.None),
				},
			};

		public static TableSchemaType From(string typeName, string format) =>
			(typeName, format) switch {
				("boolean", _) => Boolean,
				("datetime", _) => DateTime,
				("date", _) => Date,
				("time", _) => Time,
				("integer", _) => Int32,
				("number", _) => Double,
				("string", "binary") => Binary,
				("string", "uuid") => Uuid,
				("string", _) => String,
				_ => throw new NotSupportedException($"Don't know how to map '{typeName}'"),
			};
	}
}
