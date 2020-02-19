using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Threading;
using DataBoss.Linq;
using Newtonsoft.Json;

namespace DataBoss.DataPackage
{

	public class DataPackage : IDataPackageBuilder
	{
		public static string Delimiter = ";";

		public readonly List<TabularDataResource> Resources = new List<TabularDataResource>();

		class DataPackageResourceBuilder : IDataPackageResourceBuilder
		{
			readonly DataPackage package;
			readonly TabularDataResource resource;

			public DataPackageResourceBuilder(DataPackage package, TabularDataResource resource) {
				this.package = package;
				this.resource = resource;
			}

			public IDataPackageResourceBuilder AddResource(string name, Func<IDataReader> getData) =>
				package.AddResource(name, getData);

			public void Save(Func<string, Stream> createOutput, CultureInfo culture = null) =>
				package.Save(createOutput, culture);

			public DataPackage Serialize(CultureInfo culture) =>
				package.Serialize(culture);

			public IDataPackageResourceBuilder WithPrimaryKey(params string[] parts) {
				if(parts != null && parts.Length > 0)
					resource.Schema.PrimaryKey.AddRange(parts);
				return this;
			}

			public IDataPackageResourceBuilder WithForeignKey(DataPackageForeignKey fk) {
				if(!package.Resources.Any(x => x.Name == fk.Reference.Resource))
					throw new InvalidOperationException($"Missing resource '{fk.Reference.Resource}'");
				resource.Schema.ForeignKeys.Add(fk);
				return this;
			}

			public DataPackage Done() => package;
		}

		public static DataPackage Load(string path) {
			if(path.EndsWith(".zip")) 
				return LoadZip(path);

			var datapackagePath = path.EndsWith("datapackage.json") ? path : Path.Combine(path, "datapackage.json");
			var description = JsonConvert.DeserializeObject<DataPackageDescription>(File.ReadAllText(datapackagePath));
			var r = new DataPackage();

			var datapackageRoot = Path.GetDirectoryName(datapackagePath);
			r.Resources.AddRange(description.Resources.Select(x => 
				new TabularDataResource(x.Name, x.Schema, () => 
					NewCsvDataReader(
						File.OpenText(Path.Combine(datapackageRoot, x.Path)),
						x.Delimiter,
						x.Schema))));

			return r;
		}

		public static DataPackage LoadZip(string path) =>
			LoadZip(BoundMethod.Bind(File.OpenRead, path));
 	
		public static DataPackage LoadZip(Func<Stream> openZip) {
			var r = new DataPackage();
			var description = LoadZipPackageDescription(openZip);
			r.Resources.AddRange(description.Resources.Select(x => {
				return new TabularDataResource(x.Name, x.Schema,
					() => {
						var source = new ZipArchive(openZip(), ZipArchiveMode.Read);
						var csv = NewCsvDataReader(
							new StreamReader(source.GetEntry(x.Path).Open()),
							x.Delimiter,
							x.Schema);
						csv.Disposed += delegate { source.Dispose(); };
						return csv;
					});
			}));

			return r;
		}

		static DataPackageDescription LoadZipPackageDescription(Func<Stream> openZip) {
			var json = new JsonSerializer();
			using (var zip = new ZipArchive(openZip(), ZipArchiveMode.Read))
			using(var reader = new JsonTextReader(new StreamReader(zip.GetEntry("datapackage.json").Open())))
				return json.Deserialize<DataPackageDescription>(reader);
		}

		static CsvDataReader NewCsvDataReader(TextReader reader, string delimiter, TabularDataSchema schema) =>
			new CsvDataReader(
				new CsvHelper.CsvReader(
					reader,
					new CsvHelper.Configuration.Configuration { Delimiter = delimiter }),
				schema);

		public IDataPackageResourceBuilder AddResource(string name, Func<IDataReader> getData)
		{
			var resource = new TabularDataResource(name, 
				new TabularDataSchema {
					PrimaryKey = new List<string>(),
					ForeignKeys = new List<DataPackageForeignKey>(),
				}, 
				getData);
			Resources.Add(resource);
			return new DataPackageResourceBuilder(this, resource);
		}
		DataPackage IDataPackageBuilder.Done() => this;

		public void UpdateResource(string name, Func<TabularDataResource, TabularDataResource> doUpdate) {
			var found = Resources.FindIndex(x => x.Name == name);
			if(found == -1)
				throw new InvalidOperationException($"Resource '{name}' not found.");
			Resources[found] = doUpdate(Resources[found]);
		}

		public void TransformResource(string name, Action<DataReaderTransform> defineTransform) =>
			UpdateResource(name, xs => xs.Transform(defineTransform));

		public TabularDataResource GetResource(string name) => Resources.Single(x => x.Name == name);

		public void Save(Func<string, Stream> createOutput, CultureInfo culture = null) {
			var description = new DataPackageDescription();
			var decimalCharOverride = culture != null ? culture.NumberFormat.NumberDecimalSeparator : null;
			foreach (var item in Resources) {
				var resourcePath = $"{item.Name}.csv";
				using (var output = createOutput(resourcePath))
				using (var data = item.Read()) {
					var format = Enumerable.Repeat(new CsvFormatter(culture ?? CultureInfo.CurrentCulture), data.FieldCount).ToArray();

					var fields = new List<TabularDataSchemaFieldDescription>(item.Schema.Fields.Count);
					for(var i = 0; i != item.Schema.Fields.Count; ++i) {
						var x = item.Schema.Fields[i];
						if (!x.IsNumber())
							fields.Add(x);
						else {
							var customFormat = new TabularDataSchemaFieldDescription(
								x.Name,
								x.Type,
								constraints: x.Constraints,
								decimalChar: decimalCharOverride ?? x.DecimalChar);
							format[i] = new CsvFormatter(customFormat.GetNumberFormat());
							fields.Add(customFormat);
						}
					}

					description.Resources.Add(new DataPackageResourceDescription {
						Name = item.Name, 
						Path = Path.GetFileName(resourcePath),
						Delimiter = Delimiter,
						Schema = new TabularDataSchema { 
							Fields = fields,
							PrimaryKey = NullIfEmpty(item.Schema.PrimaryKey),
							ForeignKeys = NullIfEmpty(item.Schema.ForeignKeys),
						},
					});
					try {
						WriteRecords(output, data, format);
					} catch(Exception ex) {
						throw new Exception($"Failed writing {item.Name}.", ex);
					}
				}
			};

			using (var meta = new StreamWriter(createOutput("datapackage.json")))
				meta.Write(JsonConvert.SerializeObject(description, Formatting.Indented));
		}

		public DataPackage Serialize(CultureInfo culture = null) {
			var bytes = new MemoryStream();
			this.SaveZip(bytes, culture);
#if NET452
			var buffer = bytes.ToArray();
			bytes = null;
			return LoadZip(() => new MemoryStream(buffer, false));
#else
			bytes.TryGetBuffer(out var buffer);
			return LoadZip(() => new MemoryStream(buffer.Array, buffer.Offset, buffer.Count, false));
#endif
		}

		static List<T> NullIfEmpty<T>(List<T> values) =>
			values == null ? null : values.Count == 0 ? null : values;

		static CsvWriter NewCsvWriter(Stream stream, Encoding encoding) => 
			new CsvWriter(new StreamWriter(stream, encoding, 4096, leaveOpen: true));


		static readonly ConcurrentDictionary<Type, Func<CsvFormatter, object, string>> ConversionCache = new ConcurrentDictionary<Type, Func<CsvFormatter, object, string>>();
		static readonly Func<CsvFormatter, object, string> DefaultFormat = (Func<CsvFormatter, object, string>)Delegate.CreateDelegate(typeof(Func<CsvFormatter, object, string>), typeof(CsvFormatter).GetMethod("Format", new[] { typeof(object) }));

		static Func<CsvFormatter, object, string> GetFormatFunc(Type fieldType) {
			var formatBy = typeof(CsvFormatter).GetMethods(BindingFlags.Instance | BindingFlags.Public).SingleOrDefault(x => x.Name == "Format" && x.GetParameters().Single().ParameterType == fieldType);
			if (formatBy == null)
				return DefaultFormat;

			var formatArg = Expression.Parameter(typeof(CsvFormatter), "format");
			var xArg = Expression.Parameter(typeof(object), "x");
			return Expression.Lambda<Func<CsvFormatter, object, string>>(
					Expression.Call(formatArg, formatBy, Expression.Convert(xArg, fieldType)),
					formatArg, xArg)
				.Compile();
		}

		class BoundFormatter
		{
			public CsvFormatter CsvFormat;
			public Func<CsvFormatter, object, string> ToCsvString;

			public string Format(object value) => ToCsvString(CsvFormat, value);
		}

		static void WriteRecords(Stream output, IDataReader data, CsvFormatter[] format) {
			var toString = new Func<object, string>[data.FieldCount];
			for (var i = 0; i != data.FieldCount; ++i) {
				toString[i] = new BoundFormatter {
					CsvFormat = format[i],
					ToCsvString  = ConversionCache.GetOrAdd(data.GetFieldType(i), GetFormatFunc),
				}.Format;
			}

			using (var csv = NewCsvWriter(output, Encoding.UTF8)) {
				for (var i = 0; i != data.FieldCount; ++i)
					csv.WriteField(data.GetName(i));
				csv.NextRecord();
				csv.Writer.Flush();

				var reader = new RecordReader {
					DataReader = data,
				};
				reader.Start();

				var writer = new ChunkWriter {
					DataReader = data,
					Records = reader.GetConsumingEnumerable(),
					Encoding = new UTF8Encoding(false),
					FormatValue = toString,					
				};
				writer.Start();

				foreach (var item in writer.GetConsumingEnumerable())
					item.CopyTo(output);

				if (reader.Error != null)
					throw new Exception("Failed to write csv", reader.Error);
				if (writer.Error != null)
					throw new Exception("Failed to write csv", writer.Error);
			}
		}

		abstract class WorkItem
		{
			public Exception Error { get; private set; }

			protected abstract void DoWork();
			protected virtual void Cleanup() { }

			void Run() {
				try {
					DoWork();
				} catch (Exception ex) {
					Error = ex;
				} finally {
					Cleanup();
				}
			}

			public void Start() =>
				ThreadPool.QueueUserWorkItem(RunWorkItem, this);

			static void RunWorkItem(object obj) =>
				((WorkItem)obj).Run();
		}

		class RecordReader : WorkItem
		{
			public const int BufferRows = 128;

			readonly BlockingCollection<(object[] Values, int Rows)> records = new BlockingCollection<(object[], int)>(1 << 10);
			public IDataReader DataReader;

			int RowOffset(int n) => DataReader.FieldCount * n;

			public IEnumerable<(object[] Values, int Rows)> GetConsumingEnumerable() =>
				records.GetConsumingEnumerable();

			protected override void DoWork() {
				var values = new object[DataReader.FieldCount * BufferRows];
				var n = 0;
				while (DataReader.Read()) {
					var first = RowOffset(n);
					for (var i = 0; i != DataReader.FieldCount; ++i)
						values[first + i] = DataReader.IsDBNull(i) ? null : DataReader.GetValue(i);

					if (++n == BufferRows) {
						records.Add((values, n));
						n = 0;
						values = new object[DataReader.FieldCount * BufferRows];
					}
				}

				if (n != 0)
					records.Add((values, n));
			}

			protected override void Cleanup() =>
				records.CompleteAdding();
		}

		class CsvFormatter
		{
			readonly IFormatProvider formatProvider;

			public CsvFormatter(IFormatProvider formatProvider) {
				this.formatProvider = formatProvider;
			}

			public string Format(string value) => value;

			public string Format(DateTime value) {
				if(value.Kind == DateTimeKind.Unspecified)
					throw new InvalidOperationException("DateTimeKind.Unspecified not supported.");
				return value.ToUniversalTime().ToString("yyyy'-'MM'-'dd'T'HH':'mm':'ssK");
			}

			public string Format(object obj) =>
				obj is IFormattable x ? x.ToString(null, formatProvider) : obj?.ToString();
		}

		class ChunkWriter : WorkItem
		{
			readonly BlockingCollection<MemoryStream> chunks = new BlockingCollection<MemoryStream>(128);

			public IDataReader DataReader;
			public IEnumerable<(object[] Values, int Rows)> Records;
			public Func<object, string>[] FormatValue;

			public Encoding Encoding;

			int RowOffset(int n) => DataReader.FieldCount * n;

			public IEnumerable<MemoryStream> GetConsumingEnumerable() => chunks.GetConsumingEnumerable();

			protected override void DoWork() {

				var bom = Encoding.GetPreamble();
				var bufferGuess = RecordReader.BufferRows * 128;
				Records.ForEach(item => {
					if (item.Rows == 0)
						return;
					var chunk = new MemoryStream(bufferGuess);
					using (var fragment = NewCsvWriter(chunk, Encoding)) {
						for (var n = 0; n != item.Rows; ++n) {
							var first = RowOffset(n);
							for (var i = 0; i != DataReader.FieldCount; ++i) {
								var value = item.Values[first + i];
								fragment.WriteField(value == null ? string.Empty : FormatValue[i](value));
							}
							fragment.NextRecord();
						}
						fragment.Flush();
					}
					bufferGuess = Math.Max(bufferGuess, (int)chunk.Position);
					chunk.Position = bom.Length;
					chunks.Add(chunk);
				});
			}

			protected override void Cleanup() =>
				chunks.CompleteAdding();
		}
	}
}
