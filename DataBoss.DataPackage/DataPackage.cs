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
					var format = Enumerable.Repeat(culture ?? CultureInfo.CurrentCulture, data.FieldCount).Cast<IFormatProvider>().ToArray();

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
							format[i] = customFormat.GetNumberFormat();
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

		static Func<IDataRecord, int, string> GetFormatter(Type type, IFormatProvider format) {
			switch (Type.GetTypeCode(type)) {
				default: 
					if(type == typeof(TimeSpan))
						return (r, i) => { 
							if(r.IsDBNull(i))
								return null;
							return ((TimeSpan)r.GetValue(i)).ToString("hh\\:mm\\:ss");
						};
					return (r, i) => {
						var obj = r.GetValue(i);
						return obj is IFormattable x ? x.ToString(null, format) : obj?.ToString();
					};

				case TypeCode.DateTime: return (r, i) => {
					var value = r.GetDateTime(i);
					if (value.Kind == DateTimeKind.Unspecified)
						throw new InvalidOperationException("DateTimeKind.Unspecified not supported.");
					return value.ToUniversalTime().ToString("yyyy'-'MM'-'dd'T'HH':'mm':'ssK");
				};

				case TypeCode.String: return (r, i) => r.GetString(i);

				case TypeCode.Int32: return (r, i) => r.GetInt32(i).ToString(format);
				case TypeCode.Int64: return (r, i) => r.GetInt64(i).ToString(format);

				case TypeCode.Single: return (r, i) => r.GetFloat(i).ToString(format);
				case TypeCode.Double: return (r, i) => r.GetDouble(i).ToString(format);
				case TypeCode.Decimal: return (r, i) => r.GetDecimal(i).ToString(format);
			}
		}

		static void WriteRecords(Stream output, IDataReader data, IReadOnlyList<IFormatProvider> format) {
			var toString = new Func<IDataRecord, int, string>[data.FieldCount];
			for (var i = 0; i != data.FieldCount; ++i)
				toString[i] = GetFormatter(data.GetFieldType(i), format[i]);

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

			readonly BlockingCollection<IReadOnlyList<IDataRecord>> records = new BlockingCollection<IReadOnlyList<IDataRecord>>(1 << 10);
			public IDataReader DataReader;

			public IEnumerable<IReadOnlyList<IDataRecord>> GetConsumingEnumerable() =>
				records.GetConsumingEnumerable();

			protected override void DoWork() {
				var values = CreateBuffer();
				var n = 0;

				while (DataReader.Read()) {
					values.Add(ObjectDataRecord.GetRecord(DataReader));

					if (++n == BufferRows) {
						records.Add(values);
						n = 0;
						values = CreateBuffer();
					}
				}

				if (n != 0)
					records.Add(values);
			}

			List<IDataRecord> CreateBuffer() => new List<IDataRecord>(BufferRows);

			protected override void Cleanup() =>
				records.CompleteAdding();
		}

		class ObjectDataRecord : IDataRecord
		{
			readonly object[] values;
			readonly bool[] isNull;

			public static ObjectDataRecord GetRecord(IDataReader reader) {
				var fields = new object[reader.FieldCount];
				var isNull = new bool[reader.FieldCount];

				reader.GetValues(fields);
				for (var i = 0; i != isNull.Length; ++i)
					isNull[i] = reader.IsDBNull(i);

				return new ObjectDataRecord(fields, isNull);
			}

			ObjectDataRecord(object[] fields, bool[] isNull) {
				this.values = fields;
				this.isNull = isNull;
			}

			public bool IsDBNull(int i) => isNull[i];

			public object GetValue(int i) =>  values[i];

			public bool GetBoolean(int i) => (bool)values[i];
			public DateTime GetDateTime(int i) => (DateTime)values[i];
			public Guid GetGuid(int i) => (Guid)values[i];

			public byte GetByte(int i) => (byte)values[i];
			public char GetChar(int i) => (char)values[i];

			public short GetInt16(int i) => (short)values[i];
			public int GetInt32(int i) => (int)values[i];
			public long GetInt64(int i) => (long)values[i];

			public float GetFloat(int i) => (float)values[i];
			public double GetDouble(int i) => (double)values[i];
			public decimal GetDecimal(int i) => (decimal)values[i];

			public string GetString(int i) => (string)values[i];

			public object this[int i] => GetValue(i);

			public object this[string name] => throw new NotImplementedException();

			public int FieldCount => values.Length;

			public long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length) {
				throw new NotImplementedException();
			}

			public long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length) {
				throw new NotImplementedException();
			}

			public IDataReader GetData(int i) {
				throw new NotImplementedException();
			}

			public string GetDataTypeName(int i) {
				throw new NotImplementedException();
			}

			public Type GetFieldType(int i) {
				throw new NotImplementedException();
			}

			public string GetName(int i) {
				throw new NotImplementedException();
			}

			public int GetOrdinal(string name) {
				throw new NotImplementedException();
			}

			public int GetValues(object[] values) {
				throw new NotImplementedException();
			}
		}

		class ChunkWriter : WorkItem
		{
			readonly BlockingCollection<MemoryStream> chunks = new BlockingCollection<MemoryStream>(1 << 10);

			public IDataReader DataReader;
			public IEnumerable<IReadOnlyList<IDataRecord>> Records;
			public Func<IDataRecord, int, string>[] FormatValue;

			public Encoding Encoding;

			public IEnumerable<MemoryStream> GetConsumingEnumerable() => chunks.GetConsumingEnumerable();

			protected override void DoWork() {
				var bom = Encoding.GetPreamble();
				var bufferGuess = RecordReader.BufferRows * 128;

				Records.ForEach(item => {
					if (item.Count == 0)
						return;
					var chunk = new MemoryStream(bufferGuess);
					using (var fragment = NewCsvWriter(chunk, Encoding)) {
						for (var n = 0; n != item.Count; ++n) {
							var r = item[n];
							for (var i = 0; i != DataReader.FieldCount; ++i) {
								if(r.IsDBNull(i))
									fragment.NextField();
								else
									fragment.WriteField(FormatValue[i](r, i));
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
