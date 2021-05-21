using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Channels;
using System.Threading.Tasks;
using DataBoss.Data;
using DataBoss.Threading;
using DataBoss.Threading.Channels;

namespace DataBoss.DataPackage
{
	class CsvRecordWriter
	{
		readonly string delimiter;
		public readonly Encoding Encoding;

		public CsvRecordWriter(string delimiter, Encoding encoding) {
			this.delimiter = delimiter;
			this.Encoding = encoding;
		}

		CsvFragmentWriter NewFragmentWriter(Stream stream) =>
			NewFragmentWriter(NewTextWriter(stream));

		CsvFragmentWriter NewFragmentWriter(TextWriter writer) =>
			new CsvFragmentWriter(new CsvWriter(writer, delimiter, leaveOpen: true));

		public void WriteHeaderRecord(Stream stream, IDataRecord data) =>
			WriteHeaderRecord(NewTextWriter(stream), data);

		TextWriter NewTextWriter(Stream stream) =>
			new StreamWriter(stream, Encoding, DataPackage.StreamBufferSize);
		
		public void WriteHeaderRecord(TextWriter writer, IDataRecord data) {
			using var csv = NewFragmentWriter(writer);
			for (var i = 0; i != data.FieldCount; ++i)
				csv.WriteField(data.GetName(i));
			csv.NextRecord();
			csv.Flush();
		}

		public void WriteRecords(TextWriter writer, IDataReader reader, Func<IDataRecord, int, string>[] toString) {
			using var csv = NewFragmentWriter(writer);
			while (reader.Read())
				csv.WriteRecord(reader, toString);
		}

		public Task WriteChunksAsync(ChannelReader<IReadOnlyCollection<IDataRecord>> records, ChannelWriter<MemoryStream> chunks, Func<IDataRecord, int, string>[] toString) {
			var writer = new ChunkWriter(records, chunks, this) {
				FormatValue = toString,
			};
			return writer.RunAsync();
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

}
