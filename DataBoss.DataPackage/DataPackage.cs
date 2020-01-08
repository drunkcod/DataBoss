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
using Newtonsoft.Json;

namespace DataBoss.DataPackage
{
	public class TabularDataSchema
	{
		[JsonProperty("fields")]
		public List<DataPackageTabularFieldDescription> Fields;

		[JsonProperty("primaryKey", DefaultValueHandling = DefaultValueHandling.Ignore)]
		[JsonConverter(typeof(ItemOrArrayJsonConverter))]
		public List<string> PrimaryKey;

		[JsonProperty("foreignKeys", DefaultValueHandling = DefaultValueHandling.Ignore)]
		public List<DataPackageForeignKey> ForeignKeys;
	}

	public class DataPackageTabularFieldDescription
	{
		[JsonProperty("name")]
		public string Name;
		[JsonProperty("type")]
		public string Type;
	}

	public class DataPackageDescription
	{
		[JsonProperty("resources")]
		public List<DataPackageResourceDescription> Resources = new List<DataPackageResourceDescription>();

		public static DataPackageDescription Load(string path) => JsonConvert.DeserializeObject<DataPackageDescription>(File.ReadAllText(path));
	}

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
		}

		public static DataPackage Load(string path) {
			if(path.EndsWith(".zip")) 
				return LoadZip(path);

			var datapackagePath = path.EndsWith("datapackage.json") ? path : Path.Combine(path, "datapackage.json");
			var description = JsonConvert.DeserializeObject<DataPackageDescription>(File.ReadAllText(datapackagePath));
			var r = new DataPackage();

			var datapackageRoot = Path.GetDirectoryName(datapackagePath);
			r.Resources.AddRange(description.Resources.Select(x => 
				new TabularDataResource(x.Name, x.Schema, 
					() => new CsvDataReader(
						new CsvHelper.CsvReader(
							File.OpenText(Path.Combine(datapackageRoot, x.Path)),
							new CsvHelper.Configuration.Configuration { Delimiter = x.Delimiter }),
						x.Schema))));

			return r;
		}

		static DataPackage LoadZip(string path) {
			using (var zip = new ZipArchive(File.OpenRead(path), ZipArchiveMode.Read)) {
				var description = JsonConvert.DeserializeObject<DataPackageDescription>(new StreamReader(zip.GetEntry("datapackage.json").Open()).ReadToEnd());
				var r = new DataPackage();

				var datapackageRoot = string.Empty;
				r.Resources.AddRange(description.Resources.Select(x =>
					new TabularDataResource(x.Name, x.Schema,
						() =>  { 
							var source = new ZipArchive(File.OpenRead(path), ZipArchiveMode.Read);
							var csv = new CsvDataReader(
								new CsvHelper.CsvReader(
									new StreamReader(source.GetEntry(x.Path).Open()),
									new CsvHelper.Configuration.Configuration { Delimiter = x.Delimiter }),
								x.Schema);
							csv.Disposed += delegate { source.Dispose(); };
							return csv;
						})));
					

				return r;
			}
		}

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
			foreach (var item in Resources) {
				var resourcePath = $"{item.Name}.csv";
				using (var output = createOutput(resourcePath))
				using (var data = item.Read()) {
					description.Resources.Add(new DataPackageResourceDescription {
						Name = item.Name, 
						Path = Path.GetFileName(resourcePath),
						Delimiter = Delimiter,
						Schema = new TabularDataSchema { 
							Fields = item.Schema.Fields,
							PrimaryKey = NullIfEmpty(item.Schema.PrimaryKey),
							ForeignKeys = NullIfEmpty(item.Schema.ForeignKeys),
						},
					});
					try {
						WriteRecords(output, culture ?? CultureInfo.CurrentCulture, data);
					} catch(Exception ex) {
						throw new Exception($"Failed writing {item.Name}.", ex);
					}
				}
			};

			using (var meta = new StreamWriter(createOutput("datapackage.json")))
				meta.Write(JsonConvert.SerializeObject(description, Formatting.Indented));
		}

		static List<T> NullIfEmpty<T>(List<T> values) => 
			values == null ? null : values.Count == 0 ? null : values;

		static CsvWriter NewCsvWriter(Stream stream, Encoding encoding) => new CsvWriter(new StreamWriter(stream, encoding, 4096, leaveOpen: true));

		static void WriteRecords(Stream output, CultureInfo culture, IDataReader data) {

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
					Format = culture,
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

			protected abstract void DoWorkCore();

			void DoWork() {
				try {
					DoWorkCore();
				} catch (Exception ex) {
					Error = ex;
				}
			}

			public void Start() =>
				ThreadPool.QueueUserWorkItem(RunWorkItem, this);

			static void RunWorkItem(object obj) =>
				((WorkItem)obj).DoWork();
		}

		class RecordReader : WorkItem
		{

			public const int BufferRows = 1024;

			readonly BlockingCollection<(object[] Values, int Rows)> records = new BlockingCollection<(object[], int)>(1 << 10);
			public IDataReader DataReader;

			int RowOffset(int n) => DataReader.FieldCount * n;

			public IEnumerable<(object[] Values, int Rows)> GetConsumingEnumerable() =>
				records.GetConsumingEnumerable();

			protected override void DoWorkCore() {
				try {
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
				finally {
					records.CompleteAdding();
				}
			}
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
			static readonly ConcurrentDictionary<Type, Func<CsvFormatter, object, string>> ConversionCache = new ConcurrentDictionary<Type, Func<CsvFormatter, object, string>>();
			static readonly Func<CsvFormatter, object, string>  DefaultFormat = (Func<CsvFormatter, object, string>)Delegate.CreateDelegate(typeof(Func<CsvFormatter, object, string>), typeof(CsvFormatter).GetMethod("Format", new[]{ typeof(object)}));

			static Func<CsvFormatter, object, string> GetFormatFunc(Type fieldType) {
				var formatBy = typeof(CsvFormatter).GetMethods(BindingFlags.Instance | BindingFlags.Public).SingleOrDefault(x => x.Name == "Format" && x.GetParameters().Single().ParameterType == fieldType);
				if(formatBy == null)
					return DefaultFormat;

				var formatArg = Expression.Parameter(typeof(CsvFormatter), "format");
				var xArg = Expression.Parameter(typeof(object), "x");
				return Expression.Lambda<Func<CsvFormatter, object, string>>(
						Expression.Call(formatArg, formatBy, Expression.Convert(xArg, fieldType)),
						formatArg, xArg)
					.Compile();
			}

			readonly BlockingCollection<MemoryStream> chunks = new BlockingCollection<MemoryStream>(128);

			public IDataReader DataReader;
			public IEnumerable<(object[] Values, int Rows)> Records;
			public IFormatProvider Format;

			public Encoding Encoding;

			int RowOffset(int n) => DataReader.FieldCount * n;

			public IEnumerable<MemoryStream> GetConsumingEnumerable() => chunks.GetConsumingEnumerable();

			protected override void DoWorkCore() {
				var format = new CsvFormatter(Format);

				try {
					var toString = new Func<CsvFormatter, object, string>[DataReader.FieldCount];
					for (var i = 0; i != DataReader.FieldCount; ++i)
						toString[i] = ConversionCache.GetOrAdd(DataReader.GetFieldType(i), GetFormatFunc);

					var bom = Encoding.GetPreamble();
					var bufferGuess = RecordReader.BufferRows * 128;
					Records.AsParallel().AsOrdered()
					.ForAll(item => {
						if (item.Rows == 0)
							return;
						var chunk = new MemoryStream(bufferGuess);
						using (var fragment = NewCsvWriter(chunk, Encoding)) {
							for (var n = 0; n != item.Rows; ++n) {
								var first = RowOffset(n);
								for (var i = 0; i != DataReader.FieldCount; ++i) {
									var value = item.Values[first + i];
									fragment.WriteField(value == null ? string.Empty : toString[i](format, value));
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
				finally {
					chunks.CompleteAdding();
				}
			}
		}
	}
}
