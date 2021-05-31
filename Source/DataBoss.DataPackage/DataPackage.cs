using System;
using System.Buffers;
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
	public class DataPackage : IDataPackageBuilder {
		readonly List<TabularDataResource> resources = new();

		delegate bool TryGetResourceOutputPath(ResourcePath path, out string outputPath);

		internal const int StreamBufferSize = 81920;
		public const string DefaultDelimiter = ";";

		public IReadOnlyList<TabularDataResource> Resources => resources.AsReadOnly();

		class DataPackageResourceBuilder : IDataPackageResourceBuilder {
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
				if (parts != null && parts.Length > 0)
					resource.Schema.PrimaryKey.AddRange(parts);
				return this;
			}

			public IDataPackageResourceBuilder WithForeignKey(DataPackageForeignKey fk) {
				if (!package.resources.Any(x => x.Name == fk.Reference.Resource))
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
			if (path.EndsWith(".zip"))
				return LoadZip(path);

			var datapackageRoot = path.EndsWith("datapackage.json") ? Path.GetDirectoryName(path) : path;
			return Load(x => File.OpenRead(Path.Combine(datapackageRoot, x)));
		}

		static DataPackage LoadUrl(string url) {
			var source = WebResponseStream.Get(url);
			if (source.ContentType == "application/zip") {
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

		class ZipResource {
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
			if (found == -1)
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
			var writtenPaths = new HashSet<string>();

			foreach (var item in resources) {
				using var data = item.Read();
				var desc = item.GetDescription(options.Culture);
				desc.Path = item.Path;
				if (desc.Path.IsEmpty)
					desc.Path = $"{item.Name}.csv";

				desc.Dialect.Delimiter ??= DefaultDelimiter;

				description.Resources.Add(desc);

				if (!desc.Path.TryGetOutputPath(out var partPath) || writtenPaths.Contains(partPath))
					continue;

				var (outputPath, output) = options.ResourceCompression.OpenWrite(partPath, createOutput);
				desc.Path = outputPath;
				try {
					var view = DataRecordStringView.Create(desc.Schema.Fields, data, options.Culture);
					await WriteRecordsAsync(output, desc.Dialect, data, view);
				} catch (Exception ex) {
					throw new Exception($"Failed writing {item.Name}.", ex);
				} finally {
					writtenPaths.Add(outputPath);
					output.Dispose();
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

		static Task WriteRecordsAsync(Stream output, CsvDialectDescription csvDialect, IDataReader data, DataRecordStringView view) {
			var encoding = Encoding.UTF8;
			var bom = encoding.GetPreamble();
			var csv = new CsvRecordWriter(csvDialect.Delimiter, encoding);
			if (csvDialect.HasHeaderRow)
				csv.WriteHeaderRecord(output, data);

			var records = Channel.CreateBounded<(IMemoryOwner<IDataRecord>, int)>(new BoundedChannelOptions(16) {
				SingleWriter = true,
			});

			var chunks = Channel.CreateBounded<Stream>(new BoundedChannelOptions(16) {
				SingleWriter = false,
				SingleReader = true,
			});

			var cancellation = new CancellationTokenSource();
			var readerTask = new RecordReader(data.AsDataRecordReader(), records, cancellation.Token).RunAsync();
			var writerTask = csv.WriteChunksAsync(records, chunks, view);

			writerTask.ContinueWith(x => {
				if (x.IsFaulted) 
					cancellation.Cancel();
			}, TaskContinuationOptions.ExecuteSynchronously);

			return Task.WhenAll(
				readerTask, 
				writerTask,
				CopyChunks(chunks.Reader, output, bom));
		}

		internal static Task CopyChunks(ChannelReader<Stream> chunks, Stream output, byte[] bom) =>
			chunks.ForEachAsync(x => {
				try {
					if (!TryReadBom(x, bom))
						throw new InvalidOperationException("Missing BOM in fragment.");
					x.CopyTo(output);
				}
				finally {
					x.Dispose();
				}
			});

		static bool TryReadBom(Stream stream, byte[] bom) {
			var fragmentBom = new byte[bom.Length];
			for (var read = 0;  read != fragmentBom.Length;) {
				var n = stream.Read(fragmentBom, read, fragmentBom.Length - read);
				if (n == 0)
					return false;
				read += n;
			}
			return new Span<byte>(fragmentBom).SequenceEqual(bom);
		}
	}

	public class DataPackageSaveOptions
	{
		public CultureInfo Culture = null;
		public ResourceCompression ResourceCompression = ResourceCompression.None;
	}

	public static class TabularDataResourceCsvExtensions
	{
		public static void WriteCsv(this TabularDataResource self, TextWriter writer) {
			using var reader = self.Read();
			var desc = self.GetDescription();
			var view = DataRecordStringView.Create(desc.Schema.Fields, reader, null);
			var csv = new CsvRecordWriter(";", writer.Encoding);
			csv.WriteHeaderRecord(writer, reader);
			csv.WriteRecords(writer, reader, view);
		}

		public static async Task WriteCsvAsync(this TabularDataResource self, Stream output) {
			using var reader = self.Read();
			var desc = self.GetDescription();
			var view = DataRecordStringView.Create(desc.Schema.Fields, reader, null);

			var csvDialect = new CsvDialectDescription { Delimiter = ";" };

			var encoding = Encoding.UTF8;
			var bom = encoding.GetPreamble();
			var csv = new CsvRecordWriter(csvDialect.Delimiter, encoding);
			if (csvDialect.HasHeaderRow)
				csv.WriteHeaderRecord(output, reader);

			var records = Channel.CreateBounded<(IMemoryOwner<IDataRecord>, int)>(new BoundedChannelOptions(16) {
				SingleWriter = true,
			});

			var chunks = Channel.CreateBounded<Stream>(new BoundedChannelOptions(16) {
				SingleWriter = false,
				SingleReader = true,
			});

			var cancellation = new CancellationTokenSource();
			var readerTask = new RecordReader(reader.AsDataRecordReader(), records, cancellation.Token).RunAsync();
			var writerTask = csv.WriteChunksAsync(records, chunks, view);

			await Task.WhenAll(
				readerTask,
				writerTask, 
				writerTask.ContinueWith(x => {
					if (x.IsFaulted)
						cancellation.Cancel();
				}, TaskContinuationOptions.ExecuteSynchronously),
				DataPackage.CopyChunks(chunks.Reader, output, bom));
		}
	}

	class RecordReader : WorkItem
	{
		public const int BufferRows = 256;

		readonly IDataRecordReader reader;
		readonly ChannelWriter<(IMemoryOwner<IDataRecord>, int)> writer;
		readonly CancellationToken cancellation;

		public RecordReader(IDataRecordReader reader, ChannelWriter<(IMemoryOwner<IDataRecord>, int)> writer, CancellationToken cancellation) {
			this.reader = reader;
			this.writer = writer;
			this.cancellation = cancellation;
		}

		protected override void DoWork() {
			var buffer = CreateBuffer();
			var rows = buffer.Memory.Span;
			var n = 0;

			while (!cancellation.IsCancellationRequested && reader.Read()) {
				rows[n] = reader.GetRecord();

				if (++n == BufferRows) {
					writer.Write((buffer, n));
					n = 0;
					buffer = CreateBuffer();
					rows = buffer.Memory.Span;
				}
			}

			if (n != 0)
				writer.Write((buffer, n));
		}

		IMemoryOwner<IDataRecord> CreateBuffer() => MemoryPool<IDataRecord>.Shared.Rent(BufferRows);

		protected override void Cleanup() =>
			writer.Complete();
	}

}
