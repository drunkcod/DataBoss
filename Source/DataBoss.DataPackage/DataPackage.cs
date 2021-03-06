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
using CsvHelper.Configuration;
using DataBoss.Data;
using DataBoss.DataPackage.Schema;
using DataBoss.Linq;
using DataBoss.Threading;
using DataBoss.Threading.Channels;
using Newtonsoft.Json;

namespace DataBoss.DataPackage
{
	public class DataPackage : IDataPackageBuilder
	{
		readonly List<TabularDataResource> resources = new();

		delegate bool TryGetResourceOutputPath(ResourcePath path, out string outputPath);

		const int StreamBufferSize = 81920;
		public const string DefaultDelimiter = ";";

		public IReadOnlyList<TabularDataResource> Resources => resources.AsReadOnly();

		class DataPackageResourceBuilder : IDataPackageResourceBuilder
		{
			readonly DataPackage package;
			readonly TabularDataResource resource;

			public DataPackageResourceBuilder(DataPackage package, TabularDataResource resource) {
				this.package = package;
				this.resource = resource;
			}

			public IDataPackageResourceBuilder AddResource(string name, Func<IDataReader> getData) => package.AddResource(name, getData);
			public IDataPackageBuilder AddResource(Action<CsvResourceBuilder> setupResource) => package.AddResource(setupResource);

			public void Save(Func<string, Stream> createOutput, DataPackageSaveOptions options) =>
				package.Save(createOutput, options);

			public Task SaveAsync(Func<string, Stream> createOutput, DataPackageSaveOptions options) =>
				package.SaveAsync(createOutput, options);

			public DataPackage Serialize(CultureInfo culture) =>
				package.Serialize(culture);

			public Task<DataPackage> SerializeAsync(CultureInfo culture) =>
				package.SerializeAsync(culture);

			public IDataPackageResourceBuilder WithPrimaryKey(params string[] parts) {
				if(parts != null && parts.Length > 0)
					resource.Schema.PrimaryKey.AddRange(parts);
				return this;
			}

			public IDataPackageResourceBuilder WithForeignKey(DataPackageForeignKey fk) {
				if(!package.resources.Any(x => x.Name == fk.Reference.Resource))
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
			var source = WebResponseStream.Get(url);
			if(source.ContentType == "application/zip") {
				try {
					var bytes = new MemoryStream();
					source.CopyTo(bytes);
					var buff = bytes.TryGetBuffer(out var ok) ? ok : new ArraySegment<byte>(bytes.ToArray());
					return LoadZip(() => new MemoryStream(buff.Array, buff.Offset, buff.Count, writable: false));
				} finally {
					source.Dispose();
				}
			} else {
				var description = LoadPackageDescription(WebResponseStream.Get(url));
				var r = new DataPackage();
				r.AddResources(description.Resources.Select(x =>
					TabularDataResource.From(x, () =>
						CreateCsvDataReader(x, WebResponseStream.Get))));
				return r;
			}
		}

		public static DataPackage Load(Func<string, Stream> openRead) {
			var description = LoadPackageDescription(openRead("datapackage.json"));
			var r = new DataPackage();
			r.AddResources(description.Resources.Select(x =>
				TabularDataResource.From(x, () => CreateCsvDataReader(x, openRead))));

			return r;
		}

		public static DataPackage LoadZip(string path) =>
			LoadZip(BoundMethod.Bind(File.OpenRead, path));
 	
		public static DataPackage LoadZip(Func<Stream> openZip) {
			var r = new DataPackage();
			var description = LoadZipPackageDescription(openZip);
			r.AddResources(description.Resources.Select(x => 
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
			using var zip = new ZipArchive(openZip(), ZipArchiveMode.Read);
			return LoadPackageDescription(zip.GetEntry("datapackage.json").Open());
		}

		static DataPackageDescription LoadPackageDescription(Stream stream) {
			var json = new JsonSerializer();
			using var reader = new JsonTextReader(new StreamReader(stream));
			return json.Deserialize<DataPackageDescription>(reader);
		}

		static CsvDataReader CreateCsvDataReader(DataPackageResourceDescription resource, Func<string, Stream> open) =>
			CreateCsvDataReader(new StreamReader(resource.Path.OpenStream(open), Encoding.UTF8, true, StreamBufferSize),
				resource.Dialect, resource.Schema);

		static CsvDataReader CreateCsvDataReader(TextReader reader, CsvDialectDescription csvDialect, TabularDataSchema schema) =>
			new CsvDataReader(
				new CsvHelper.CsvReader(
					reader,
					new CsvConfiguration(CultureInfo.InvariantCulture) { 
						Delimiter = csvDialect.Delimiter ?? CsvDialectDescription.DefaultDelimiter 
					}),
				schema,
				hasHeaderRow: csvDialect.HasHeaderRow);

		public IDataPackageResourceBuilder AddResource(string name, Func<IDataReader> getData) =>
			AddResource(new CsvResourceOptions {
				Name = name,
				Path = Path.ChangeExtension(name, "csv")
			}, getData);

		public IDataPackageResourceBuilder AddResource(CsvResourceOptions item) {
			var parts = item.Path
				.Select(path => resources.First(x => x.Path.Count == 1 && x.Path == path))
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
			AddResource(resource);
			return new DataPackageResourceBuilder(this, resource);
		}

		public IDataPackageBuilder AddResource(Action<CsvResourceBuilder> setupResource) {
			var newResource = new CsvResourceBuilder();
			setupResource(newResource);
			return AddResource(newResource.Build());
		}

		public IDataPackageBuilder AddResource(TabularDataResource item) {
			if (TryGetResource(item.Name, out var _))
				throw new InvalidOperationException($"Can't add duplicate resource {item.Name}.");
			resources.Add(item);
			return this;
		}

		void AddResources(IEnumerable<TabularDataResource> items) =>
			resources.AddRange(items);

		DataPackage IDataPackageBuilder.Done() => this;

		public void UpdateResource(string name, Func<TabularDataResource, TabularDataResource> doUpdate) {
			var found = resources.FindIndex(x => x.Name == name);
			if(found == -1)
				throw new InvalidOperationException($"Resource '{name}' not found.");
			resources[found] = doUpdate(resources[found]);
		}

		public void AddOrUpdateResource(string name, Action<CsvResourceBuilder> setupResource, Func<TabularDataResource, TabularDataResource> doUpdate) {
			var found = resources.FindIndex(x => x.Name == name);
			if (found != -1)
				resources[found] = doUpdate(resources[found]);
			else {
				var newResource = new CsvResourceBuilder();
				setupResource(newResource.WithName(name));
				AddResource(newResource.Build());
			}
		}

		public void TransformResource(string name, Action<DataReaderTransform> defineTransform) =>
			UpdateResource(name, xs => xs.Transform(defineTransform));

		public TabularDataResource GetResource(string name) => 
			TryGetResource(name, out var found) ? found : throw new InvalidOperationException($"Invalid resource name '{name}'");

		public bool TryGetResource(string name, out TabularDataResource found) =>
			(found = resources.SingleOrDefault(x => x.Name == name)) != null;

		public bool RemoveResource(string name) =>
			RemoveResource(name, ConstraintsBehavior.Check, out var _);

		public bool RemoveResource(string name, out TabularDataResource value) =>
			RemoveResource(name, ConstraintsBehavior.Check, out value);

		public bool RemoveResource(string name, ConstraintsBehavior constraintsBehavior, out TabularDataResource value) {
			if (!TryGetResource(name, out value))
				return false;

			var references = AllForeignKeys()
				.Where(x => x.ForeignKey.Resource == name)
				.ToList();

			switch (constraintsBehavior) {
				case ConstraintsBehavior.Check:

					if (references.Any())
						throw new InvalidOperationException($"Can't remove resource {name}, it's referenced by: {string.Join(", ", references.Select(x => x.Resource.Name))}.");
					break;

				case ConstraintsBehavior.Drop:
					foreach (var item in references)
						item.Resource.Schema.ForeignKeys.RemoveAll(x => x.Reference.Resource == name);
					break;
			}

			resources.Remove(value);
			return true;
		}

		IEnumerable<(TabularDataResource Resource, DataPackageKeyReference ForeignKey)> AllForeignKeys() =>
			resources.SelectMany(xs => xs.Schema?.ForeignKeys.EmptyIfNull().Select(x => (Resource: xs, ForeignKey: x.Reference))); 

		public void Save(Func<string, Stream> createOutput, CultureInfo culture = null) =>
			Save(createOutput, new DataPackageSaveOptions { Culture = culture });

		public void Save(Func<string, Stream> createOutput, DataPackageSaveOptions options) =>
			TaskRunner.Run(() => SaveAsync(createOutput, options));

		public Task SaveAsync(Func<string, Stream> createOutput, CultureInfo culture = null) =>
			SaveAsync(createOutput, new DataPackageSaveOptions { Culture = culture });

		public async Task SaveAsync(Func<string, Stream> createOutput, DataPackageSaveOptions options) {
			var description = new DataPackageDescription();
			var decimalCharOverride = options.Culture?.NumberFormat.NumberDecimalSeparator;
			var defaultFormatter = new RecordFormatter(options.Culture ?? CultureInfo.InvariantCulture);
			var writtenPaths = new HashSet<string>();

			foreach (var item in resources) {
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
				if (options.ResourceCompression.TryGetOutputPath(desc.Path, out var outputPath) && !writtenPaths.Contains(outputPath)) {
					desc.Path = outputPath;
					var output = options.ResourceCompression.WrapWrite(createOutput(outputPath));
					try {
						await WriteRecordsAsync(output, desc.Dialect, data, toString);
					} catch (Exception ex) {
						throw new Exception($"Failed writing {item.Name}.", ex);
					} finally {
						writtenPaths.Add(outputPath);
						output.Dispose();
					}
				}
			}

			using var meta = new StreamWriter(createOutput("datapackage.json"));
			meta.Write(JsonConvert.SerializeObject(description, Formatting.Indented));
		}

		public DataPackage Serialize(CultureInfo culture = null) {
			var store = new InMemoryDataPackageStore();
			Save(store.OpenWrite, culture);
			return Load(store.OpenRead);
		}

		public async Task<DataPackage> SerializeAsync(CultureInfo culture = null) {
			var store = new InMemoryDataPackageStore();
			await SaveAsync(store.OpenRead, culture);
			return Load(store.OpenRead);
		}

		class CsvRecordWriter
		{
			readonly string delimiter;
			public readonly Encoding Encoding;

			public CsvRecordWriter(string delimiter, Encoding encoding) {
				this.delimiter = delimiter;
				this.Encoding = encoding;
			}

			public CsvFragmentWriter NewFragmentWriter(Stream stream) =>
				new CsvFragmentWriter(new CsvWriter(new StreamWriter(stream, Encoding, StreamBufferSize, leaveOpen: true), delimiter));

			public Task WriteChunksAsync(ChannelReader<IReadOnlyCollection<IDataRecord>> records, ChannelWriter<MemoryStream> chunks, Func<IDataRecord, int, string>[] toString) {
				var writer = new ChunkWriter(records, chunks, this) {
					FormatValue = toString,
				};
				return writer.RunAsync();
			}

			class ChunkWriter : WorkItem
			{
				readonly ChannelReader<IReadOnlyCollection<IDataRecord>> records;
				readonly ChannelWriter<MemoryStream> chunks;
				readonly CsvRecordWriter csv;
				readonly int bomLength;
				int chunkCapacity = 4 * 4096;

				public ChunkWriter(ChannelReader<IReadOnlyCollection<IDataRecord>> records, ChannelWriter<MemoryStream> chunks, CsvRecordWriter csv) {
					this.records = records;
					this.chunks = chunks;
					this.csv = csv;
					this.bomLength = csv.Encoding.GetPreamble().Length;
				}

				public Func<IDataRecord, int, string>[] FormatValue;
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
					using (var fragment = csv.NewFragmentWriter(chunk))
						foreach (var r in item)
							fragment.WriteRecord(r, FormatValue);

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

		class CsvFragmentWriter : IDisposable
		{
			readonly CsvWriter csv;

			public CsvFragmentWriter(CsvWriter csv) {
				this.csv = csv;
			}

			public void Dispose() => csv.Dispose();

			public void WriteField(string value) => csv.WriteField(value);
			public void NextField() => csv.NextField();

			public void WriteRecord(IDataRecord r, Func<IDataRecord, int, string>[] recordFormat) {
				for (var i = 0; i != r.FieldCount; ++i) {
					if (r.IsDBNull(i))
						NextField();
					else
						WriteField(recordFormat[i](r, i));
				}
				NextRecord();
			}
			public void NextRecord() => csv.NextRecord();
			public void Flush() => csv.Writer.Flush();
		}

		static Task WriteRecordsAsync(Stream output, CsvDialectDescription csvDialect, IDataReader data, Func<IDataRecord, int, string>[] toString) {
			var csv = new CsvRecordWriter(csvDialect.Delimiter, Encoding.UTF8);
			if (csvDialect.HasHeaderRow) {
				using var headerFragment = csv.NewFragmentWriter(output);
				WriteHeaderRecord(headerFragment, data);
			}

			var records = Channel.CreateBounded<IReadOnlyCollection<IDataRecord>>(new BoundedChannelOptions(1024) {
				SingleWriter = true,
			});

			var chunks = Channel.CreateBounded<MemoryStream>(new BoundedChannelOptions(1024) {
				SingleWriter = false,
				SingleReader = true,
			});

			var cancellation = new CancellationTokenSource();
			var readerTask = new RecordReader(data.AsDataRecordReader(), records, cancellation.Token).RunAsync();
			var writerTask = csv.WriteChunksAsync(records, chunks, toString);

			writerTask.ContinueWith(x => {
				if (x.IsFaulted) 
					cancellation.Cancel();
			}, TaskContinuationOptions.ExecuteSynchronously);

			return Task.WhenAll(
				readerTask, 
				writerTask,
				chunks.Reader.ForEachAsync(x => x.CopyTo(output)));
		}

		static void WriteHeaderRecord(CsvFragmentWriter csv, IDataRecord data) {
			for (var i = 0; i != data.FieldCount; ++i)
				csv.WriteField(data.GetName(i));
			csv.NextRecord();
			csv.Flush();
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
	}

	public class DataPackageSaveOptions
	{
		public CultureInfo Culture = null;
		public ResourceCompression ResourceCompression = ResourceCompression.None;
	}
}
