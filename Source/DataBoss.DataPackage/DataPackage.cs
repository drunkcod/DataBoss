using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using DataBoss.Data;
using DataBoss.Linq;
using DataBoss.Threading.Channels;
using Newtonsoft.Json;

namespace DataBoss.DataPackage
{
	public class CsvResourceOptions
	{
		public string Name;
		public ResourcePath Path;

		public bool HasHeaderRow = true;
	}

	public partial class DataPackage : IDataPackageBuilder
	{
		delegate bool TryGetResourceOutputPath(ResourcePath path, out string outputPath);

		const int StreamBufferSize = 81920;
		public const string DefaultDelimiter = ";";

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

			public void Save(Func<string, Stream> createOutput, DataPackageSaveOptions options) =>
				package.Save(createOutput, options);

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

			public IDataPackageResourceBuilder WithDelimiter(string delimiter) {
				(resource as CsvDataResource).Delimiter = delimiter;
				return this;
			}

			public DataPackage Done() => package;
		}

		public static DataPackage Load(string path) {
			if (Regex.IsMatch(path, "http(s?)://"))
				return LoadUrl(path);
			if(path.EndsWith(".zip")) 
				return LoadZip(path);

			var datapackageRoot = path.EndsWith("datapackage.json") ? Path.GetDirectoryName(path) : path;
			return Load(x => File.OpenRead(Path.Combine(datapackageRoot, x)));
		}

		static DataPackage LoadUrl(string url) {
			DataPackageDescription description;
			using (var reader = new JsonTextReader(new StreamReader(WebResponseStream.Get(url)))) {
				var json = new JsonSerializer();
				description = json.Deserialize<DataPackageDescription>(reader);
			}

			var r = new DataPackage();
			r.Resources.AddRange(description.Resources.Select(x =>
				TabularDataResource.From(x, () =>
					CreateCsvDataReader(x, WebResponseStream.Get))));

			return r;
		}

		public static DataPackage Load(Func<string, Stream> openRead) {
			DataPackageDescription description;
			using(var reader = new JsonTextReader(new StreamReader(openRead("datapackage.json")))) {
				var json = new JsonSerializer();
				description = json.Deserialize<DataPackageDescription>(reader);
			}

			var r = new DataPackage();
			r.Resources.AddRange(description.Resources.Select(x =>
				TabularDataResource.From(x, () => CreateCsvDataReader(x, openRead))));

			return r;
		}

		public static DataPackage LoadZip(string path) =>
			LoadZip(BoundMethod.Bind(File.OpenRead, path));
 	
		public static DataPackage LoadZip(Func<Stream> openZip) {
			var r = new DataPackage();
			var description = LoadZipPackageDescription(openZip);
			r.Resources.AddRange(description.Resources.Select(x => 
				TabularDataResource.From(x, new ZipResource(openZip, x).GetData)));

			return r;
		}

		class ZipResource
		{
			readonly Func<Stream> openZip;
			readonly DataPackageResourceDescription resource;

			public ZipResource(Func<Stream> openZip, DataPackageResourceDescription resource) {
				this.openZip = openZip;
				this.resource = resource;
			}

			public IDataReader GetData() {
				var source = new ZipArchive(openZip(), ZipArchiveMode.Read);
				var csv = CreateCsvDataReader(resource, x => source.GetEntry(x).Open());
				csv.Disposed += delegate { source.Dispose(); };
				return csv;
			}
		}

		static DataPackageDescription LoadZipPackageDescription(Func<Stream> openZip) {
			var json = new JsonSerializer();
			using var zip = new ZipArchive(openZip(), ZipArchiveMode.Read);
			using var reader = new JsonTextReader(new StreamReader(zip.GetEntry("datapackage.json").Open()));
				
			return json.Deserialize<DataPackageDescription>(reader);
		}

		static CsvDataReader CreateCsvDataReader(DataPackageResourceDescription resource, Func<string, Stream> open) =>
			CreateCsvDataReader(new StreamReader(resource.Path.OpenStream(open), Encoding.UTF8, true, StreamBufferSize),
				resource.Dialect, resource.Schema);

		static CsvDataReader CreateCsvDataReader(TextReader reader, CsvDialectDescription csvDialect, TabularDataSchema schema) =>
			new CsvDataReader(
				new CsvHelper.CsvReader(
					reader,
					new CsvHelper.Configuration.CsvConfiguration(CultureInfo.InvariantCulture) { Delimiter = csvDialect.Delimiter ?? "," }),
				schema, 
				hasHeaderRow: csvDialect.HasHeaderRow);

		public IDataPackageResourceBuilder AddResource(string name, Func<IDataReader> getData) =>
			AddResource(new CsvResourceOptions {
				Name = name,
				Path = Path.ChangeExtension(name, "csv")
			}, getData);

		public IDataPackageResourceBuilder AddResource(CsvResourceOptions item) {
			var parts = item.Path
				.Select(path => Resources.First(x => x.Path.Count == 1 && x.Path == path))
				.Select(x => x.Read());

			return AddResource(item, () => new MultiDataReader(parts));
		}

		public IDataPackageResourceBuilder AddResource(CsvResourceOptions item, Func<IDataReader> getData) {
			var resource = TabularDataResource.From(
				new DataPackageResourceDescription {
					Format = "csv",
					Name = item.Name,
					Path = item.Path,
					Schema = new TabularDataSchema {
						PrimaryKey = new List<string>(),
						ForeignKeys = new List<DataPackageForeignKey>(),
					},
					Dialect = new CsvDialectDescription {
						HasHeaderRow = item.HasHeaderRow,
					}
				}, getData);
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

		public TabularDataResource GetResource(string name) => 
			Resources.SingleOrDefault(x => x.Name == name) ?? throw new InvalidOperationException($"Invalid resource name '{name}'");

		public void Save(Func<string, Stream> createOutput, CultureInfo culture = null) =>
			Save(createOutput, new DataPackageSaveOptions { Culture = culture });
		
		public void Save(Func<string, Stream> createOutput, DataPackageSaveOptions options) {
			var description = new DataPackageDescription();
			var decimalCharOverride = options.Culture?.NumberFormat.NumberDecimalSeparator;
			var defaultFormatter = new RecordFormatter(options.Culture ?? CultureInfo.InvariantCulture);
			var writtenPaths = new HashSet<string>();
			TryGetResourceOutputPath getOutputPath = TryGetOutputPath;
			var createResourceStream = createOutput;

			if(options.ResourceCompression == ResourceCompression.GZip) {
				getOutputPath = TryGetGZipOutputPath;
				createResourceStream = path => new GZipStream(createOutput(path), CompressionLevel.Optimal);
			}

			foreach (var item in Resources) {
				using var data = item.Read();
				var desc = item.GetDescription();
				desc.Path = item.Path;
				if (desc.Path.IsEmpty)
					desc.Path = $"{item.Name}.csv" ;

				var fieldCount = item.Schema.Fields.Count;
				var toString = new Func<IDataRecord, int, string>[fieldCount];

				for (var i = 0; i != fieldCount; ++i) {
					var field = desc.Schema.Fields[i];
					var fieldFormatter = defaultFormatter;
					if (field.IsNumber()) {
						field = desc.Schema.Fields[i] = new TabularDataSchemaFieldDescription(
							field.Name,
							field.Type,
							constraints: field.Constraints,
							decimalChar: decimalCharOverride ?? field.DecimalChar);
						fieldFormatter = new RecordFormatter(field.GetNumberFormat());
					}
					toString[i] = fieldFormatter.GetFormatter(data.GetFieldType(i), field);
				}

				desc.Dialect.Delimiter ??= DefaultDelimiter;

				description.Resources.Add(desc);
				if (getOutputPath(desc.Path, out var outputPath) && !writtenPaths.Contains(outputPath)) {
					desc.Path = outputPath;
					var output = createResourceStream(outputPath);
					try {
						WriteRecords(output, desc.Dialect, data, toString);
					}
					catch (Exception ex) {
						throw new Exception($"Failed writing {item.Name}.", ex);
					}
					finally {
						writtenPaths.Add(outputPath);
						output.Dispose();
					}
				}
			}

			using var meta = new StreamWriter(createOutput("datapackage.json"));
			meta.Write(JsonConvert.SerializeObject(description, Formatting.Indented));
		}

		static bool TryGetOutputPath(ResourcePath path, out string outputPath) {
			if(path.Count != 1) {
				outputPath = null;
				return false;
			}
			outputPath = path.First();
			return true;
		}

		static bool TryGetGZipOutputPath(ResourcePath path, out string outputPath) {
			if (!TryGetOutputPath(path, out outputPath))
				return false;
			outputPath = Path.ChangeExtension(outputPath, Path.GetExtension(outputPath) + ".gz");
			return true;
		}

		public DataPackage Serialize(CultureInfo culture = null) {
			var bytes = new MemoryStream();
			this.SaveZip(bytes, culture);
			bytes.TryGetBuffer(out var buffer);
			return LoadZip(() => new MemoryStream(buffer.Array, buffer.Offset, buffer.Count, false));
		}

		static CsvWriter NewCsvWriter(Stream stream, Encoding encoding, string delimiter) => 
			new CsvWriter(new StreamWriter(stream, encoding, StreamBufferSize, leaveOpen: true), delimiter);

		static void WriteRecords(Stream output, CsvDialectDescription csvDialect, IDataReader data, Func<IDataRecord, int, string>[] toString) {

			if(csvDialect.HasHeaderRow)
				WriteHeaderRecord(output, Encoding.UTF8, csvDialect.Delimiter, data);

			var records = Channel.CreateBounded<IReadOnlyCollection<IDataRecord>>(new BoundedChannelOptions(1024) {
				SingleWriter = true,
			});

			var chunks = Channel.CreateBounded<MemoryStream>(new BoundedChannelOptions(1024) {
				SingleWriter = false,
				SingleReader = true,
			});

			var cancellation = new CancellationTokenSource();
			var reader = new RecordReader(data.AsDataRecordReader(), records, cancellation.Token);
			var writer = new ChunkWriter(records, chunks, new UTF8Encoding(false)) {
				Delimiter = csvDialect.Delimiter,
				FormatValue = toString,
			};
			var writeTask = writer.RunAsync();

			writeTask.ContinueWith(_ => cancellation.Cancel(), TaskContinuationOptions.OnlyOnFaulted);

			Task.WaitAll(
				reader.RunAsync(), 
				writeTask,
				chunks.Reader.ForEachAsync(x => x.CopyTo(output)));
		}

		static void WriteHeaderRecord(Stream output, Encoding encoding, string delimiter, IDataReader data) {
			using var csv = NewCsvWriter(output, encoding, delimiter);
			for (var i = 0; i != data.FieldCount; ++i)
				csv.WriteField(data.GetName(i));
			csv.NextRecord();
			csv.Writer.Flush();
		}

		abstract class WorkItem
		{
			Thread thread;

			protected abstract void DoWork();
			protected virtual void Cleanup() { }

			public Task RunAsync() {
				if(thread != null && thread.IsAlive)
					throw new InvalidOperationException("WorkItem already started.");
				thread = new Thread(Run) {
					IsBackground = true,
					Name = GetType().Name,
				};
				var tsc = new TaskCompletionSource<int>();
				thread.Start(tsc);

				return tsc.Task;
			}

			void Run(object obj) {
				var tsc = (TaskCompletionSource<int>)obj;
				try {
					DoWork();
					tsc.SetResult(0);
				} catch (Exception ex) {
					tsc.SetException(new Exception("CSV writing failed.", ex));
				} finally {
					Cleanup();
				}
			}

			public void Join() => thread.Join();
		}

		class RecordReader : WorkItem
		{
			public const int BufferRows = 8192;

			readonly IDataRecordReader reader;
			readonly ChannelWriter<IReadOnlyCollection<IDataRecord>> writer;
			readonly CancellationToken cancellation;
 
			public RecordReader(IDataRecordReader reader, ChannelWriter<IReadOnlyCollection<IDataRecord>> writer, CancellationToken cancellation) {
				this.reader = reader;
				this.writer = writer;
				this.cancellation = cancellation;
			}

			protected override void DoWork() {
				var values = CreateBuffer();
				var n = 0;
				while(!cancellation.IsCancellationRequested && reader.Read()) {
					values.Add(reader.GetRecord());

					if (++n == BufferRows) {
						writer.Write(values);
						n = 0;
						values = CreateBuffer();
					}
				}

				if (n != 0)
					writer.Write(values);
			}

			List<IDataRecord> CreateBuffer() => new List<IDataRecord>(BufferRows);

			protected override void Cleanup() =>
				writer.Complete();
		}

		class ChunkWriter : WorkItem
		{
			readonly ChannelReader<IReadOnlyCollection<IDataRecord>> records;
			readonly ChannelWriter<MemoryStream> chunks;
			readonly Encoding encoding;
			readonly int bomLength;
			int chunkCapacity = 4 * 4096; 

			public ChunkWriter(ChannelReader<IReadOnlyCollection<IDataRecord>> records, ChannelWriter<MemoryStream> chunks, Encoding encoding) {
				this.records = records;
				this.chunks = chunks;
				this.encoding = encoding;
				this.bomLength = encoding.GetPreamble().Length;
			}

			public Func<IDataRecord, int, string>[] FormatValue;
			public string Delimiter;
			public int MaxWorkers = 1;

			protected override void DoWork() {
				if (MaxWorkers == 1)
					records.ForEach(WriteRecords);
				else 
					records.GetConsumingEnumerable().AsParallel()
						.WithDegreeOfParallelism(MaxWorkers)
						.ForAll(WriteRecords);
			}

			void WriteRecords(IReadOnlyCollection<IDataRecord> item) {
				if (item.Count == 0)
					return;

				var chunk = new MemoryStream(chunkCapacity);
				using (var fragment = NewCsvWriter(chunk, encoding, Delimiter)) {
					foreach (var r in item) {
						for (var i = 0; i != r.FieldCount; ++i) {
							if (r.IsDBNull(i))
								fragment.NextField();
							else
								fragment.WriteField(FormatValue[i](r, i));
						}
						fragment.NextRecord();
					}
				}
				if (chunk.Position != 0) {
					chunkCapacity = Math.Max(chunkCapacity, chunk.Capacity);
					WriteChunk(chunk);
				}
			}

			void WriteChunk(MemoryStream chunk) {
				chunk.Position = bomLength;
				chunks.Write(chunk);
			}

			protected override void Cleanup() =>
				chunks.Complete();
		}
	}

	public class DataPackageSaveOptions
	{
		public CultureInfo Culture = null;
		public ResourceCompression ResourceCompression;
	}

	public enum ResourceCompression
	{
		None,
		GZip,
	}
}
